"""Reporting views built on top of the SCD2 dimension (what BI tools would query)."""

from __future__ import annotations

import duckdb

VIEWS = {
    # number of communes per département at every loaded snapshot (point-in-time join)
    "mart_communes_by_departement": """
        CREATE OR REPLACE VIEW mart_communes_by_departement AS
        SELECT a.snapshot_date, d.dep, count(*) AS communes
        FROM load_audit a
        JOIN dim_commune d
          ON d.valid_from <= a.snapshot_date
         AND (d.valid_to IS NULL OR d.valid_to > a.snapshot_date)
        WHERE d.typecom = 'COM'
        GROUP BY ALL
    """,
    # which columns drive the changes, per snapshot
    "mart_change_summary": """
        CREATE OR REPLACE VIEW mart_change_summary AS
        SELECT snapshot_date, change_type,
               coalesce(trim(col), '') AS changed_column, count(*) AS n
        FROM fact_commune_change
        LEFT JOIN LATERAL (SELECT unnest(string_split(changed_columns, ',')) AS col) ON true
        GROUP BY ALL
    """,
    # departements that lost the most communes over the whole history
    "mart_departement_consolidation": """
        CREATE OR REPLACE VIEW mart_departement_consolidation AS
        WITH bounds AS (SELECT min(snapshot_date) AS first, max(snapshot_date) AS last
                        FROM load_audit)
        SELECT m.dep,
               max(m.communes) FILTER (WHERE m.snapshot_date = b.first) AS communes_first,
               max(m.communes) FILTER (WHERE m.snapshot_date = b.last) AS communes_last
        FROM mart_communes_by_departement m, bounds b
        GROUP BY m.dep
    """,
}


def build_marts(con: duckdb.DuckDBPyConnection) -> None:
    for sql in VIEWS.values():
        con.execute(sql)
