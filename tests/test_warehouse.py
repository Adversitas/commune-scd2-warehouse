import gzip
from datetime import date

import pytest

from cogwh.cli import build
from cogwh.marts import build_marts
from cogwh.quality import run_checks
from cogwh.validate import load_events, recette
from cogwh.warehouse import (
    SnapshotOrderError,
    as_of,
    connect,
    load_snapshot,
    snapshot_date_from_name,
)

HEADER = (
    '"TYPECOM","COM","REG","DEP","CTCD","ARR","TNCC","NCC","NCCENR","LIBELLE","CAN","COMPARENT"'
)


def row(typecom, com, libelle, dep="01", can="0101", parent=""):
    return (
        f'"{typecom}","{com}","84","{dep}","{dep}D","011","0","{libelle.upper()}",'
        f'"{libelle}","{libelle}","{can}","{parent}"'
    )


def write(path, rows, gz=False):
    text = "\n".join([HEADER, *rows]) + "\n"
    if gz:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write(text)
    else:
        path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def snapshots(tmp_path):
    y1 = write(
        tmp_path / "v_commune_2023.csv",
        [row("COM", "01001", "Alpha"), row("COM", "01002", "Beta"), row("COM", "01003", "Gamma")],
    )
    # 2024: Beta renamed, Gamma merged into Alpha as a delegated commune, Delta created,
    # Alpha moved to another canton (attachment change, not an official commune event)
    y2 = write(
        tmp_path / "v_commune_2024.csv.gz",
        [
            row("COM", "01001", "Alpha", can="0199"),
            row("COM", "01002", "Beta-sur-Mer"),
            row("COMD", "01003", "Gamma", parent="01001"),
            row("COM", "01004", "Delta"),
        ],
        gz=True,
    )
    return y1, y2


def test_snapshot_date_from_name():
    assert snapshot_date_from_name("data/v_commune_2025.csv.gz") == date(2025, 1, 1)
    with pytest.raises(ValueError):
        snapshot_date_from_name("communes.csv")


def test_scd2_versions_and_counts(snapshots):
    con = connect()
    r1 = load_snapshot(con, snapshots[0])
    assert (r1.inserted, r1.updated, r1.deleted) == (3, 0, 0)
    r2 = load_snapshot(con, snapshots[1])
    # inserts: COMD 01003 + COM 01004 ; updates: 01001 (canton) + 01002 (name) ; delete: COM 01003
    assert (r2.inserted, r2.updated, r2.deleted, r2.unchanged) == (2, 2, 1, 0)
    beta = con.execute(
        "SELECT libelle, valid_from, valid_to, is_current FROM dim_commune "
        "WHERE com = '01002' ORDER BY valid_from"
    ).fetchall()
    assert beta == [
        ("Beta", date(2023, 1, 1), date(2024, 1, 1), False),
        ("Beta-sur-Mer", date(2024, 1, 1), None, True),
    ]
    changed = con.execute(
        "SELECT changed_columns FROM fact_commune_change WHERE com = '01002' "
        "AND change_type = 'update'"
    ).fetchone()[0]
    assert changed == "ncc,nccenr,libelle"


def test_reload_is_idempotent_and_order_is_enforced(snapshots):
    con = connect()
    load_snapshot(con, snapshots[0])
    load_snapshot(con, snapshots[1])
    rows = con.execute("SELECT count(*) FROM dim_commune").fetchone()[0]
    again = load_snapshot(con, snapshots[1])
    assert again.skipped
    assert con.execute("SELECT count(*) FROM dim_commune").fetchone()[0] == rows
    with pytest.raises(SnapshotOrderError):
        load_snapshot(con, snapshots[0])


def test_duplicate_keys_are_rejected_and_rolled_back(tmp_path):
    con = connect()
    bad = write(
        tmp_path / "v_commune_2023.csv", [row("COM", "01001", "A"), row("COM", "01001", "B")]
    )
    with pytest.raises(ValueError, match="duplicated"):
        load_snapshot(con, bad)
    assert con.execute("SELECT count(*) FROM load_audit").fetchone()[0] == 0


def test_as_of_point_in_time(snapshots):
    con = connect()
    for s in snapshots:
        load_snapshot(con, s)
    before = as_of(con, date(2023, 6, 1))
    after = as_of(con, date(2024, 6, 1))
    assert set(before.libelle) == {"Alpha", "Beta", "Gamma"}
    assert "Beta-sur-Mer" in set(after.libelle) and len(after) == 4


def test_contracts_pass_on_consistent_history(snapshots):
    con = connect()
    for s in snapshots:
        load_snapshot(con, s)
    assert all(c.passed for c in run_checks(con)), run_checks(con)


def test_contract_detects_orphan_delegated_commune(tmp_path):
    con = connect()
    load_snapshot(
        con, write(tmp_path / "v_commune_2023.csv", [row("COMD", "01009", "X", parent="01999")])
    )
    failed = {c.name for c in run_checks(con) if not c.passed}
    assert failed == {"child_points_to_current_parent"}


def test_marts(snapshots):
    con = connect()
    for s in snapshots:
        load_snapshot(con, s)
    build_marts(con)
    counts = dict(
        con.execute("SELECT snapshot_date, communes FROM mart_communes_by_departement").fetchall()
    )
    assert counts == {date(2023, 1, 1): 3, date(2024, 1, 1): 3}
    cols = {
        r[0]
        for r in con.execute(
            "SELECT changed_column FROM mart_change_summary WHERE change_type = 'update'"
        ).fetchall()
    }
    assert {"can", "libelle"} <= cols


def test_recette_against_official_events(snapshots, tmp_path):
    events = tmp_path / "v_mvt_commune_2024.csv"
    events.write_text(
        '"MOD","DATE_EFF","TYPECOM_AV","COM_AV","TNCC_AV","NCC_AV","NCCENR_AV","LIBELLE_AV",'
        '"TYPECOM_AP","COM_AP","TNCC_AP","NCC_AP","NCCENR_AP","LIBELLE_AP"\n'
        '"10","2024-01-01","COM","01002","0","BETA","Beta","Beta","COM","01002","0",'
        '"BETA SUR MER","Beta-sur-Mer","Beta-sur-Mer"\n'
        '"32","2024-01-01","COM","01003","0","GAMMA","Gamma","Gamma","COMD","01003","0",'
        '"GAMMA","Gamma","Gamma"\n'
        '"32","2024-01-01","COM","01001","0","ALPHA","Alpha","Alpha","COM","01001","0",'
        '"ALPHA","Alpha","Alpha"\n'
        '"20","2024-01-01","COM","01004","0","DELTA","Delta","Delta","COM","01004","0",'
        '"DELTA","Delta","Delta"\n'
        '"10","2019-01-01","COM","01001","0","OLD","Old","Old","COM","01001","0",'
        '"ALPHA","Alpha","Alpha"\n',
        encoding="utf-8",
    )
    con = connect()
    for s in snapshots:
        load_snapshot(con, s)
    load_events(con, events)
    r = recette(con, date(2023, 1, 1), date(2024, 1, 1))
    assert r.official_events == 4  # the 2019 event is outside the window
    # expected keys: COM 01002, COM 01003, COMD 01003 (identity changes); 01004 creation has the
    # same before/after identity in INSEE's log so it is not an "expected" key
    assert r.expected_keys == 3 and r.matched == 3 and r.recall == 1.0
    # detected identity changes: those 3 + the creation of COM 01004 -> 1 unexplained
    assert r.detected_identity_keys == 4 and r.unexplained_examples == ["COM:01004"]
    assert (r.late_event, r.parent_recoded, r.unexplained) == (0, 0, 1)
    assert r.explained_share == pytest.approx(0.75)
    assert r.attachment_only_updates == 1  # Alpha's canton change


def test_build_end_to_end(snapshots, tmp_path):
    raw = snapshots[0].parent
    summary = build(str(tmp_path / "wh.duckdb"), raw, tmp_path / "reports")
    assert len(summary["loads"]) == 2 and all(v == 0 for v in summary["checks"].values())
    assert (tmp_path / "reports" / "results.json").exists()
