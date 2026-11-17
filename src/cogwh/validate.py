"""Recette: compare the changes detected by the SCD2 load with INSEE's official event log.

INSEE publishes every commune event (renaming, merger, restoration...) in ``v_mvt_commune``. For
the events whose effective date falls between two snapshots, we derive the natural keys whose row
*must* change and check that the incremental load caught them:

* a key renamed in place (same type and code before/after) must be updated;
* a key that disappears into another entity (before != after) must be closed, unless the same key
  is also the "after" side of an event in the window (it survives as the chef-lieu / absorber);
* a key that is created (creation, restoration, code change, new delegated / associated commune)
  must be inserted.

Absorbing communes are deliberately *not* expected to change: their own row usually stays
identical when they absorb a neighbour.

Every detected identity change that is not expected is classified instead of being silently
counted as an error:

* ``late_event``: the key has an INSEE event dated *before* the window, which the previous COG
  file had not applied yet (a real lag between the event log and the yearly files);
* ``parent_recoded``: only ``comparent`` changed and the new parent is a key touched in the window;
* ``unexplained``: everything else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import duckdb

from .warehouse import IDENTITY

EVENT_LABELS = {
    "10": "changement de nom",
    "20": "création",
    "21": "rétablissement",
    "30": "suppression",
    "31": "fusion simple",
    "32": "création de commune nouvelle",
    "33": "fusion association",
    "34": "fusion association -> fusion simple",
    "35": "suppression de commune déléguée",
    "41": "changement de code (département)",
    "50": "changement de code (chef-lieu)",
    "70": "commune associée -> déléguée",
    "71": "rétablissement commune déléguée",
}


@dataclass
class Recette:
    snapshot_date: date
    official_events: int
    expected_keys: int
    detected_identity_keys: int
    matched: int
    late_event: int
    parent_recoded: int
    unexplained: int
    attachment_only_updates: int
    missed_examples: list[str] = field(default_factory=list)
    unexplained_examples: list[str] = field(default_factory=list)
    events_by_type: dict[str, int] = field(default_factory=dict)

    @property
    def recall(self) -> float:
        return self.matched / self.expected_keys if self.expected_keys else 1.0

    @property
    def precision(self) -> float:
        """Detected identity changes explained by an event *inside* the window."""
        return self.matched / self.detected_identity_keys if self.detected_identity_keys else 1.0

    @property
    def explained_share(self) -> float:
        """Detected identity changes explained by any rule (window, late event, parent recode)."""
        if not self.detected_identity_keys:
            return 1.0
        explained = self.matched + self.late_event + self.parent_recoded
        return explained / self.detected_identity_keys


def load_events(con: duckdb.DuckDBPyConnection, path: Path | str) -> None:
    con.execute(
        "CREATE OR REPLACE TABLE ref_mvt_commune AS "
        "SELECT * REPLACE (DATE_EFF::DATE AS DATE_EFF) "
        "FROM read_csv(?, header = true, all_varchar = true)",
        [str(path)],
    )


def _keys(con: duckdb.DuckDBPyConnection, sql: str, limit: int = 5) -> list[str]:
    return [f"{t}:{c}" for t, c in con.execute(f"{sql} ORDER BY 2, 1 LIMIT {limit}").fetchall()]


def recette(con: duckdb.DuckDBPyConnection, previous: date, snapshot: date) -> Recette:
    con.execute(
        "CREATE OR REPLACE TEMP TABLE ev AS SELECT * FROM ref_mvt_commune "
        "WHERE DATE_EFF > ? AND DATE_EFF <= ?",
        [previous, snapshot],
    )
    official = con.execute("SELECT count(*) FROM ev").fetchone()[0]
    by_type = dict(con.execute("SELECT MOD, count(*) FROM ev GROUP BY 1 ORDER BY 1").fetchall())
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE expected AS
        WITH moving AS (
            SELECT * FROM ev
            WHERE (TYPECOM_AV, COM_AV, coalesce(LIBELLE_AV, ''))
               IS DISTINCT FROM (TYPECOM_AP, COM_AP, coalesce(LIBELLE_AP, ''))
        ),
        after_keys AS (SELECT DISTINCT TYPECOM_AP AS typecom, COM_AP AS com FROM ev)
        -- renamed in place
        SELECT TYPECOM_AP AS typecom, COM_AP AS com FROM moving
        WHERE TYPECOM_AV = TYPECOM_AP AND COM_AV = COM_AP
        UNION
        -- disappearing keys that do not survive as an "after" key
        SELECT TYPECOM_AV, COM_AV FROM moving
        WHERE (TYPECOM_AV, COM_AV) IS DISTINCT FROM (TYPECOM_AP, COM_AP)
          AND (TYPECOM_AV, COM_AV) NOT IN (SELECT (typecom, com) FROM after_keys)
        UNION
        -- created keys
        SELECT TYPECOM_AP, COM_AP FROM moving
        WHERE (TYPECOM_AV, COM_AV) IS DISTINCT FROM (TYPECOM_AP, COM_AP)
          AND (MOD IN ('20', '21', '41', '50', '71') OR TYPECOM_AP IN ('COMD', 'COMA'))
        """
    )
    identity_regex = "(^|,)(" + "|".join(IDENTITY) + ")(,|$)"
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE detected AS
        SELECT typecom, com,
               bool_and(change_type = 'update' AND changed_columns = 'comparent') AS parent_only
        FROM fact_commune_change
        WHERE snapshot_date = ?
          AND (change_type <> 'update' OR regexp_matches(changed_columns, ?))
        GROUP BY ALL
        """,
        [snapshot, identity_regex],
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE classified AS
        SELECT d.typecom, d.com,
            CASE
              WHEN e.com IS NOT NULL THEN 'matched'
              WHEN EXISTS (SELECT 1 FROM ref_mvt_commune m
                           WHERE m.DATE_EFF <= ? AND m.TYPECOM_AV = d.typecom
                             AND m.COM_AV = d.com) THEN 'late_event'
              WHEN d.parent_only AND EXISTS (
                     SELECT 1 FROM dim_commune c JOIN ev
                       ON ev.COM_AP = c.comparent OR ev.COM_AV = c.comparent
                     WHERE c.typecom = d.typecom AND c.com = d.com AND c.valid_from = ?)
                THEN 'parent_recoded'
              ELSE 'unexplained'
            END AS status
        FROM detected d LEFT JOIN expected e USING (typecom, com)
        """,
        [previous, snapshot],
    )
    status = dict(con.execute("SELECT status, count(*) FROM classified GROUP BY 1").fetchall())
    attachment_only = con.execute(
        "SELECT count(*) FROM fact_commune_change WHERE snapshot_date = ? "
        "AND change_type = 'update' AND NOT regexp_matches(changed_columns, ?)",
        [snapshot, identity_regex],
    ).fetchone()[0]
    return Recette(
        snapshot_date=snapshot,
        official_events=official,
        expected_keys=con.execute("SELECT count(*) FROM expected").fetchone()[0],
        detected_identity_keys=con.execute("SELECT count(*) FROM detected").fetchone()[0],
        matched=status.get("matched", 0),
        late_event=status.get("late_event", 0),
        parent_recoded=status.get("parent_recoded", 0),
        unexplained=status.get("unexplained", 0),
        attachment_only_updates=attachment_only,
        missed_examples=_keys(
            con,
            "SELECT typecom, com FROM expected "
            "EXCEPT SELECT typecom, com FROM classified WHERE status = 'matched'",
        ),
        unexplained_examples=_keys(
            con, "SELECT typecom, com FROM classified WHERE status = 'unexplained'"
        ),
        events_by_type={f"{k} {EVENT_LABELS.get(k, '?')}": v for k, v in by_type.items()},
    )
