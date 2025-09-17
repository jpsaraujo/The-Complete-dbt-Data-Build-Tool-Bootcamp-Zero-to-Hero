{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

--Only run this filter condition if we're doing an incremental run on the table base on review_date
{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }}) --"this" refers to the model being built
{% endif %}