
WITH source_to_target_lineage AS (
    SELECT
        modified.value:objectName::STRING AS target_table,
        modified.value:objectId::INT AS target_table_id,
        read.value:objectName::STRING AS source_table,
        read.value:objectId::INT AS source_table_id,
        h.query_id,
        h.query_start_time,
        h.user_name
    FROM
        snowflake.account_usage.access_history h,
        LATERAL FLATTEN(input => h.objects_modified) modified,
        LATERAL FLATTEN(input => h.base_objects_accessed) read
    WHERE
        -- Filter for operations that actually modify a table
        modified.value:objectDomain::STRING = 'Table'
        -- Ensure the source is also a table to track table-to-table lineage
        AND read.value:objectDomain::STRING = 'Table'
        -- Exclude self-references (e.g., UPDATE T1 SET col = col + 1)
        AND target_table_id != source_table_id
)
SELECT
    l.source_table,
    l.target_table,
    l.query_start_time,
    l.user_name,
    q.query_text
FROM
    source_to_target_lineage l
JOIN
    snowflake.account_usage.query_history q
    ON l.query_id = q.query_id
ORDER BY
    l.query_start_time DESC
LIMIT 100;



select * from   snowflake.account_usage.access_history ;