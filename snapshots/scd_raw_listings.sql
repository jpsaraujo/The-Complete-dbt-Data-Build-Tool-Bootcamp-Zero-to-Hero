{% snapshot scd_raw_listings %}

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
    listing_url,	
    name,
    room_type,
	minimum_nights,
    host_id,
    price,
    created_at,
  -- Converte a coluna `updated_at` para `TIMESTAMP`
  CAST(updated_at AS TIMESTAMP) as updated_at
FROM {{ source('proj-dbt-airbnb', 'listings') }}

{% endsnapshot %}

