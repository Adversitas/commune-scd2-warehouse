"""Incremental SCD Type 2 load of the INSEE Code Officiel Géographique (communes).

Every yearly COG file is a full snapshot of French communes on 1 January. Loading the snapshots in
order into ``dim_commune`` keeps the full history: a row is closed (``valid_to`` set,
``is_current`` false) when its tracked attributes change or the commune disappears, and a new
version is opened. The SQL is written so that each step maps 1:1 to a Snowflake / Databricks
``MERGE`` (see ``sql/snowflake_merge.sql``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

NATURAL_KEY = ("typecom", "com")
TRACKED = ("reg", "dep", "ctcd", "arr", "tncc", "ncc", "nccenr", "libelle", "can", "comparent")
IDENTITY = ("tncc", "ncc", "nccenr", "libelle", "comparent")  # changed by official commune events
ATTACHMENT = ("reg", "dep", "ctcd", "arr", "can")  # administrative attachments

DDL = f"""
CREATE SEQUENCE IF NOT EXISTS seq_commune_sk START 1;
CREATE TABLE IF NOT EXISTS dim_commune (
    commune_sk   BIGINT PRIMARY KEY DEFAULT nextval('seq_commune_sk'),
    typecom      VARCHAR NOT NULL,
    com          VARCHAR NOT NULL,
    {", ".join(f"{c} VARCHAR" for c in TRACKED)},
    hashdiff     VARCHAR NOT NULL,
    valid_from   DATE NOT NULL,
    valid_to     DATE,
    is_current   BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS load_audit (
    snapshot_date DATE PRIMARY KEY,
    source_file   VARCHAR NOT NULL,
    source_rows   BIGINT NOT NULL,
    inserted      BIGINT NOT NULL,
    updated       BIGINT NOT NULL,
    deleted       BIGINT NOT NULL,
    unchanged     BIGINT NOT NULL,
    loaded_at     TIMESTAMP DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS fact_commune_change (
    snapshot_date   DATE NOT NULL,
    typecom         VARCHAR NOT NULL,
    com             VARCHAR NOT NULL,
    change_type     VARCHAR NOT NULL,     -- insert | update | delete
    changed_columns VARCHAR               -- comma separated, updates only
);
"""


class SnapshotOrderError(ValueError):
    """Raised when a snapshot older than (or equal to) the last loaded one is loaded."""


@dataclass
class LoadResult:
    snapshot_date: date
    source_rows: int
    inserted: int
    updated: int
    deleted: int
    unchanged: int
    skipped: bool = False


def connect(path: str | Path = ":memory:") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(DDL)
    return con


def snapshot_date_from_name(path: Path | str) -> date:
    m = re.search(r"(\d{4})", Path(path).name)
    if not m:
        raise ValueError(f"cannot infer COG year from {path}")
    return date(int(m.group(1)), 1, 1)


def _hash_expr(alias: str = "") -> str:
    p = f"{alias}." if alias else ""
    return "md5(concat_ws('|', " + ", ".join(f"coalesce({p}{c}, '')" for c in TRACKED) + "))"


def stage(con: duckdb.DuckDBPyConnection, csv_path: Path | str) -> int:
    """Read one snapshot into ``stg_commune`` (all VARCHAR, empty string -> NULL)."""
    cols = ", ".join(f"nullif(trim({c.upper()}), '') AS {c}" for c in (*NATURAL_KEY, *TRACKED))
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_commune AS
        SELECT *, {_hash_expr()} AS hashdiff FROM (
            SELECT {cols}
            FROM read_csv(?, header = true, all_varchar = true)
        )
        """,
        [str(csv_path)],
    )
    dupes = con.execute(
        "SELECT count(*) FROM (SELECT typecom, com FROM stg_commune "
        "GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        raise ValueError(f"{csv_path}: {dupes} duplicated natural keys in the snapshot")
    return con.execute("SELECT count(*) FROM stg_commune").fetchone()[0]


def load_snapshot(
    con: duckdb.DuckDBPyConnection, csv_path: Path | str, snapshot: date | None = None
) -> LoadResult:
    """Apply one snapshot as an SCD2 delta. Re-loading an already-loaded snapshot is a no-op."""
    snapshot = snapshot or snapshot_date_from_name(csv_path)
    last = con.execute("SELECT max(snapshot_date) FROM load_audit").fetchone()[0]
    if last is not None and snapshot == last:
        row = con.execute(
            "SELECT source_rows FROM load_audit WHERE snapshot_date = ?", [snapshot]
        ).fetchone()
        return LoadResult(snapshot, row[0], 0, 0, 0, 0, skipped=True)
    if last is not None and snapshot < last:
        raise SnapshotOrderError(f"snapshot {snapshot} is older than last loaded {last}")

    n = stage(con, csv_path)
    key_join = " AND ".join(f"d.{k} = s.{k}" for k in NATURAL_KEY)
    diff_cols = " || ".join(
        f"CASE WHEN d.{c} IS DISTINCT FROM s.{c} THEN '{c},' ELSE '' END" for c in TRACKED
    )
    con.execute("BEGIN TRANSACTION")
    try:
        # 1. change log (computed before the dimension is touched)
        for sql in (
            f"""INSERT INTO fact_commune_change
            SELECT ?::DATE, s.typecom, s.com, 'insert', NULL
            FROM stg_commune s
            WHERE NOT EXISTS (SELECT 1 FROM dim_commune d WHERE d.is_current AND {key_join})""",
            f"""INSERT INTO fact_commune_change
            SELECT ?::DATE, s.typecom, s.com, 'update', rtrim({diff_cols}, ',')
            FROM stg_commune s JOIN dim_commune d ON d.is_current AND {key_join}
            WHERE d.hashdiff <> s.hashdiff""",
            f"""INSERT INTO fact_commune_change
            SELECT ?::DATE, d.typecom, d.com, 'delete', NULL
            FROM dim_commune d
            WHERE d.is_current
              AND NOT EXISTS (SELECT 1 FROM stg_commune s WHERE {key_join})""",
        ):
            con.execute(sql, [snapshot])
        counts = dict(
            con.execute(
                "SELECT change_type, count(*) FROM fact_commune_change "
                "WHERE snapshot_date = ? GROUP BY 1",
                [snapshot],
            ).fetchall()
        )
        # 2. close changed and deleted versions  (WHEN MATCHED ... UPDATE in a MERGE)
        con.execute(
            f"""
            UPDATE dim_commune AS d SET valid_to = ?::DATE, is_current = false
            WHERE d.is_current AND (
                NOT EXISTS (SELECT 1 FROM stg_commune s WHERE {key_join})
                OR EXISTS (SELECT 1 FROM stg_commune s WHERE {key_join}
                           AND s.hashdiff <> d.hashdiff)
            )
            """,
            [snapshot],
        )
        # 3. open new versions for inserted and changed keys  (WHEN NOT MATCHED ... INSERT)
        cols = ", ".join((*NATURAL_KEY, *TRACKED, "hashdiff"))
        con.execute(
            f"""
            INSERT INTO dim_commune ({cols}, valid_from, valid_to, is_current)
            SELECT {", ".join(f"s.{c}" for c in (*NATURAL_KEY, *TRACKED, "hashdiff"))},
                   ?::DATE, NULL, true
            FROM stg_commune s
            WHERE NOT EXISTS (SELECT 1 FROM dim_commune d WHERE d.is_current AND {key_join})
            """,
            [snapshot],
        )
        ins, upd, dele = counts.get("insert", 0), counts.get("update", 0), counts.get("delete", 0)
        result = LoadResult(snapshot, n, ins, upd, dele, n - ins - upd)
        con.execute(
            "INSERT INTO load_audit (snapshot_date, source_file, source_rows, inserted, updated,"
            " deleted, unchanged) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [snapshot, Path(csv_path).name, n, ins, upd, dele, result.unchanged],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return result


def as_of(con: duckdb.DuckDBPyConnection, day: date):
    """Point-in-time view of the dimension (what the COG looked like on ``day``)."""
    return con.execute(
        "SELECT * FROM dim_commune WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to > ?) "
        "ORDER BY typecom, com",
        [day, day],
    ).df()
