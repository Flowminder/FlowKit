-- Create an empty view containing the columns msisdn, event_time, cell_id.
CREATE OR REPLACE VIEW {{ get_extract_view(ds_nodash) }} AS
SELECT
    NULL::TEXT as msisdn,
    NULL::TIMESTAMPTZ as event_time,
    NULL::TEXT as cell_id
LIMIT 0;
