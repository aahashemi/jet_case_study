{{ config(materialized='incremental') }}

SELECT
    d.comic_num,
    round(1.0 + random() * 9.0, 1) as customer_review_rating,
FROM {{ ref('dim_comic') }} d

{% if is_incremental() %}
  LEFT JOIN {{ this }} f ON d.comic_num = f.comic_num
  WHERE f.comic_num IS NULL
{% endif %}