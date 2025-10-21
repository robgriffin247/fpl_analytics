with source as (
    select * from {{ ref("int_standings") }}
)

select * from source