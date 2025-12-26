{{ config(
    materialized='view',
) }}

SELECT
    d.comic_num,
    d.title,
    d.safe_title,
    d.image_url,
    d.alt_text,
    d.raw_transcript,
    d.external_link,
    d.news_section,
    d.release_date,
    fc.cost_euro,
    fv.total_view,
    fr.total_rating
FROM {{ ref('dim_comic') }} d
LEFT JOIN {{ ref('fct_comic_cost') }} fc 
    ON d.comic_num = fc.comic_num
LEFT JOIN {{ ref('fct_comic_view') }} fv 
    ON d.comic_num = fv.comic_num
LEFT JOIN {{ ref('fct_comic_review') }} fr 
    ON d.comic_num = fr.comic_num
