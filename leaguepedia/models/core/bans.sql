with source as (
      select * from {{ source('staging', 'scoreboardgames') }}
),
renamed as (
    select
        {{ adapter.quote("GameId") }},
        {{adapter.quote("Team1Bans")}},
        {{adapter.quote("Team2Bans")}}
    from source
),

temp as (
    select
        {{ adapter.quote("GameId") }},
        ARRAY[string_to_array("Team1Bans", ',')]::text[] as "Team1Bans",
        ARRAY[string_to_array("Team2Bans", ',')]::text[] as "Team2Bans"
    from renamed
),

unnested as (
    select "GameId", blue.name as "blue", red.name2 as "red", blue.ord as "order"
    from temp, unnest("Team1Bans") with ordinality blue(name, ord), unnest("Team2Bans") with ordinality red(name2, ord)
    where blue.ord = red.ord
)

select * from unnested