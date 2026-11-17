"""Command line: ``cogwh {build,history,asof}``."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path

from .marts import build_marts
from .quality import run_checks
from .validate import load_events, recette
from .warehouse import RAW_DIR, as_of, connect, load_snapshot, snapshot_date_from_name


def build(db: str, raw_dir: Path, report_dir: Path | None) -> dict:
    con = connect(db)
    snapshots = sorted(raw_dir.glob("v_commune_*.csv*"), key=snapshot_date_from_name)
    if not snapshots:
        raise SystemExit(f"no v_commune_YYYY.csv[.gz] in {raw_dir}")
    loads = []
    for path in snapshots:
        start = time.perf_counter()
        res = load_snapshot(con, path)
        loads.append({**asdict(res), "seconds": round(time.perf_counter() - start, 3)})
    build_marts(con)
    checks = run_checks(con)

    recettes = []
    events = sorted(raw_dir.glob("v_mvt_commune_*.csv*"))
    if events:
        load_events(con, events[-1])
        dates = [snapshot_date_from_name(p) for p in snapshots]
        for prev, cur in zip(dates, dates[1:], strict=False):
            r = recette(con, prev, cur)
            recettes.append(
                {
                    **asdict(r),
                    "recall": r.recall,
                    "precision": r.precision,
                    "explained_share": r.explained_share,
                }
            )

    summary = {
        "loads": loads,
        "dimension_rows": con.execute("SELECT count(*) FROM dim_commune").fetchone()[0],
        "checks": {c.name: c.failures for c in checks},
        "recette": recettes,
        "change_summary": con.execute(
            "SELECT snapshot_date, change_type, changed_column, n FROM mart_change_summary "
            "WHERE snapshot_date > (SELECT min(snapshot_date) FROM load_audit) "
            "ORDER BY snapshot_date, n DESC"
        ).fetchall(),
        "top_consolidation": con.execute(
            "SELECT dep, communes_first, communes_last, communes_first - communes_last AS lost "
            "FROM mart_departement_consolidation ORDER BY lost DESC, dep LIMIT 5"
        ).fetchall(),
    }
    con.close()
    if report_dir:
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "results.json").write_text(
            json.dumps(summary, indent=2, default=str, ensure_ascii=False), encoding="utf-8"
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cogwh", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="load every snapshot in order, run contracts and recette")
    b.add_argument("--db", default="warehouse.duckdb")
    b.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    b.add_argument("--reports", type=Path, default=Path("reports"))
    h = sub.add_parser("history", help="all SCD2 versions of one commune code")
    h.add_argument("com")
    h.add_argument("--db", default="warehouse.duckdb")
    a = sub.add_parser("asof", help="the commune as it was on a given day")
    a.add_argument("day", type=date.fromisoformat)
    a.add_argument("com")
    a.add_argument("--db", default="warehouse.duckdb")
    args = parser.parse_args(argv)

    if args.cmd == "build":
        Path(args.db).unlink(missing_ok=True)
        s = build(args.db, args.raw_dir, args.reports)
        for load in s["loads"]:
            print(
                f"{load['snapshot_date']}: {load['source_rows']} rows | +{load['inserted']} "
                f"~{load['updated']} -{load['deleted']} ={load['unchanged']} "
                f"({load['seconds']} s)"
            )
        failed = {k: v for k, v in s["checks"].items() if v}
        print(f"contracts: {len(s['checks']) - len(failed)}/{len(s['checks'])} passed {failed}")
        for r in s["recette"]:
            print(
                f"recette {r['snapshot_date']}: recall {r['recall']:.3f} precision "
                f"{r['precision']:.3f} explained {r['explained_share']:.3f} "
                f"({r['matched']}/{r['expected_keys']} official keys, late {r['late_event']}, "
                f"parent recoded {r['parent_recoded']}, unexplained {r['unexplained']})"
            )
        return 1 if failed else 0

    import duckdb

    con = duckdb.connect(args.db, read_only=True)
    cols = "typecom, com, libelle, dep, can, comparent, valid_from, valid_to, is_current"
    if args.cmd == "history":
        df = con.execute(
            f"SELECT {cols} FROM dim_commune WHERE com = ? ORDER BY typecom, valid_from",
            [args.com],
        ).df()
    else:
        df = as_of(con, args.day)
        df = df[df.com == args.com][cols.split(", ")]
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
