WITH raw_hosts AS (
    SELECT
        *
    FROM
       --proj-dbt-airbnb.RAW.RAW_HOSTS
       {{source('proj-dbt-airbnb', 'hosts')}}
)
SELECT
    id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts