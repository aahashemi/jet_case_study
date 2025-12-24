{{ 
    config(materialized='incremental') 
}}

SELECT
    d.comic_num,
    round(random() * 10000, 0) as total_views,
FROM {{ ref('dim_comic') }} d

{% if is_incremental() %}
  LEFT JOIN {{ this }} f ON d.comic_num = f.comic_num
  WHERE f.comic_num IS NULL
{% endif %}