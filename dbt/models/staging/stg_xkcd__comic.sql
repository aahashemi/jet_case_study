{{ 
    config(
    materialized='incremental'
) }}

with source as (
    select * from {{ source('landing_zone', 'raw_comic') }}
),

parsed_json as (
    select 
        cast(json_extract(raw_json, '$.num') as integer) as comic_num,
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
        cast(ingestion_time as timestamp) as ingestion_time,
        cast(http_status as integer) as http_status,
        current_timestamp as processed_time
    from source

    {% if is_incremental() %}
    where cast(ingestion_time as timestamp) > (select max(ingestion_time) from {{ this }})
    {% endif %}
)

select * from parsed_json




