{{ 
    config(
        materialized='table'
    ) 
}}

with staging as (
    select * from {{ ref('stg_comic') }}
),

final as (
    select
        *
        -- adding some business logic
        case 
            when external_link = '' or external_link is null then 0 
            else 1 
        end as has_external_link,

        

    from staging
)

select * from final