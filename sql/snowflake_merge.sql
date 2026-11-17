-- Snowflake / Databricks equivalent of cogwh.warehouse.load_snapshot (one snapshot = one run).
-- :snapshot_date is the 1 January of the COG year; STG_COMMUNE is the staged yearly file with a
-- HASHDIFF column = MD5 of the tracked attributes (same expression as in the Python code).

-- 1) close versions that changed or disappeared, and open versions for changed keys
MERGE INTO DIM_COMMUNE d
USING (
    -- changed or new keys: one row to insert (MERGE_KEY NULL forces the NOT MATCHED branch)
    SELECT NULL AS MERGE_TYPECOM, NULL AS MERGE_COM, s.*
    FROM STG_COMMUNE s
    LEFT JOIN DIM_COMMUNE c
      ON c.IS_CURRENT AND c.TYPECOM = s.TYPECOM AND c.COM = s.COM
    WHERE c.COM IS NULL OR c.HASHDIFF <> s.HASHDIFF
    UNION ALL
    -- changed keys: one row to close the current version
    SELECT s.TYPECOM, s.COM, s.*
    FROM STG_COMMUNE s
    JOIN DIM_COMMUNE c
      ON c.IS_CURRENT AND c.TYPECOM = s.TYPECOM AND c.COM = s.COM
    WHERE c.HASHDIFF <> s.HASHDIFF
) src
ON d.IS_CURRENT AND d.TYPECOM = src.MERGE_TYPECOM AND d.COM = src.MERGE_COM
WHEN MATCHED THEN UPDATE SET VALID_TO = :snapshot_date, IS_CURRENT = FALSE
WHEN NOT MATCHED THEN INSERT (TYPECOM, COM, REG, DEP, CTCD, ARR, TNCC, NCC, NCCENR, LIBELLE, CAN,
                              COMPARENT, HASHDIFF, VALID_FROM, VALID_TO, IS_CURRENT)
VALUES (src.TYPECOM, src.COM, src.REG, src.DEP, src.CTCD, src.ARR, src.TNCC, src.NCC, src.NCCENR,
        src.LIBELLE, src.CAN, src.COMPARENT, src.HASHDIFF, :snapshot_date, NULL, TRUE);

-- 2) close keys that are no longer in the snapshot (soft delete)
UPDATE DIM_COMMUNE d SET VALID_TO = :snapshot_date, IS_CURRENT = FALSE
WHERE d.IS_CURRENT
  AND NOT EXISTS (SELECT 1 FROM STG_COMMUNE s WHERE s.TYPECOM = d.TYPECOM AND s.COM = d.COM);
