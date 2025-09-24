{% snapshot scd_raw_hosts %}

{{
   config(
       target_schema='DEV',
       unique_key='id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select 
    id,
    name,
    is_superhost,
    created_at,
    CAST(updated_at AS TIMESTAMP) as updated_at
 FROM  {{ source('proj-dbt-airbnb', 'hosts') }}

{% endsnapshot %}