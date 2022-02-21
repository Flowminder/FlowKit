BEGIN;

DROP TABLE staging_table_{{ds_nodash}};
DROP FOREIGN TABLE IF EXISTS call_table_{{ds_nodash}};
DROP FOREIGN TABLE IF EXISTS sms_table_{{ds_nodash}};
DROP FOREIGN TABLE IF EXISTS location_table_{{ds_nodash}};
DROP FOREIGN TABLE IF EXISTS mds_table_{{ds_nodash}};
DROP FOREIGN TABLE IF EXISTS topup_table_{{ds_nodash}};

-- Vacuum or other upkeep here may be sensible?

COMMIT;
