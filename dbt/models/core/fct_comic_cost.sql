{{ config(materialized='incremental') }}

SELECT
    d.comic_num,
    length(safe_title) * 5.0 as cost_euro,
FROM {{ ref('dim_comic') }} d

{% if is_incremental() %}
  LEFT JOIN {{ this }} f ON d.comic_num = f.comic_num
  WHERE f.comic_num IS NULL
{% endif %}