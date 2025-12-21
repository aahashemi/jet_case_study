{{
  config(
    materialized='incremental',
    unique_key='comic_id'
  )
}}


with source as (
    select * from {{ source('raw_comic_data_source', 'raw_comic') }}
),

parsed_json as (
    select
        cast(json_extract(raw_json, '$.num') as integer) as comic_id,
        cast(json_extract(raw_json, '$.title') as text) as title,
        cast(json_extract(raw_json, '$.safe_title') as text) as safe_title,
        cast(json_extract(raw_json, '$.img') as text) as image_url,
        cast(json_extract(raw_json, '$.alt') as text) as alt_text,
        cast(json_extract(raw_json, '$.transcript') as text) as raw_transcript,
        cast(json_extract(raw_json, '$.link') as text) as external_link,
        cast(json_extract(raw_json, '$.news') as text) as news_section,
        cast(json_extract(raw_json, '$.year') as integer) as release_year,
        cast(json_extract(raw_json, '$.month') as integer) as release_month,
        cast(json_extract(raw_json, '$.day') as integer) as release_day,
    from source
)

select * from parsed_json

{% if is_incremental() %}
  -- only look at comic_ids higher than the ones we already have
  and comic_id > (select max(comic_id) from {{ this }})
{% endif %}