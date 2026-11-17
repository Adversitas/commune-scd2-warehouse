"""Data contracts on the SCD2 dimension. Each check returns the number of offending rows."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

CHECKS: dict[str, str] = {
    "one_current_version_per_key": """
        SELECT count(*) FROM (
            SELECT typecom, com FROM dim_commune WHERE is_current GROUP BY ALL HAVING count(*) > 1
        )""",
    "no_overlapping_validity_intervals": """
        SELECT count(*) FROM dim_commune a JOIN dim_commune b
          ON a.typecom = b.typecom AND a.com = b.com AND a.commune_sk < b.commune_sk
         AND a.valid_from < coalesce(b.valid_to, DATE '9999-12-31')
         AND b.valid_from < coalesce(a.valid_to, DATE '9999-12-31')""",
    "valid_to_after_valid_from": """
        SELECT count(*) FROM dim_commune WHERE valid_to IS NOT NULL AND valid_to <= valid_from""",
    "closed_rows_are_not_current": """
        SELECT count(*) FROM dim_commune WHERE (valid_to IS NULL) <> is_current""",
    "current_rows_match_last_snapshot": """
        SELECT abs((SELECT count(*) FROM dim_commune WHERE is_current)
                 - (SELECT source_rows FROM load_audit ORDER BY snapshot_date DESC LIMIT 1))""",
    "libelle_not_null": """
        SELECT count(*) FROM dim_commune WHERE libelle IS NULL""",
    "com_departement_code_format": """
        SELECT count(*) FROM dim_commune
        WHERE typecom = 'COM' AND NOT regexp_full_match(dep, '([0-9]{2}|2A|2B|97[0-9])')""",
    "child_points_to_current_parent": """
        SELECT count(*) FROM dim_commune c
        WHERE c.is_current AND c.typecom IN ('COMD', 'COMA', 'ARM')
          AND NOT EXISTS (SELECT 1 FROM dim_commune p WHERE p.is_current AND p.typecom = 'COM'
                          AND p.com = c.comparent)""",
}


@dataclass
class CheckResult:
    name: str
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0


def run_checks(con: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    return [CheckResult(name, con.execute(sql).fetchone()[0]) for name, sql in CHECKS.items()]
