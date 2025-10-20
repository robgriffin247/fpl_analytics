with 
players as (
    select * from {{ ref("int_players") }}
)

select * from players
