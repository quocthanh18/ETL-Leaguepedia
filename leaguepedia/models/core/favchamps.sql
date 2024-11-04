with source as (
    select *
    from {{source('staging', 'players') }}
),

main as (
    select
    "Player" as "player",
    ARRAY[string_to_array("FavChamps", ',')]::text[] as "Champion"
    from source
),

main1 as (
    select "player", "name"
    from main, unnest("Champion") as "name"
)

select * from main1