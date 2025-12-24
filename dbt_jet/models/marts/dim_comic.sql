{{ config(
    materialized='incremental',
    unique_key='comic_num',
    incremental_strategy='merge',
    merge_update_columns=[
        'title', 
        'safe_title', 
        'image_url', 
        'alt_text'
    ]
) }}

with comic_ranked as (
    select *,
           row_number() over (
               partition by comic_num
               order by processed_time desc
           ) as rn
    from {{ ref('stage_comic') }}
    where http_status = 200 and comic_num is not null
),

comic_deduped as (
    select *
    from comic_ranked
    where rn = 1
),

augmented as (
    select 
        comic_num,
        title,
        safe_title,
        image_url,
        alt_text,
        raw_transcript,
        external_link,
        news_section,
        make_date(release_year, release_month, release_day) as release_date,        
    from comic_deduped
)

select * from augmented