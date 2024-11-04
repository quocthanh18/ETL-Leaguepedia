with source as (
      select * from {{ source('staging', 'scoreboardgames') }}
),
renamed as (
    select
        {{ adapter.quote("GameId") }},
        {{adapter.quote("Team1Picks")}},
        {{adapter.quote("Team2Picks")}}
    from source
),

temp as (
    select
        {{ adapter.quote("GameId") }},
        ARRAY[string_to_array("Team1Picks", ',')]::text[] as "Team1Picks",
        ARRAY[string_to_array("Team2Picks", ',')]::text[] as "Team2Picks"
    from renamed
),

unnested as (
    select "GameId", blue.name as "blue", red.name2 as "red", {{ map_role('blue.ord')}} as "role"
    from temp, unnest("Team1Picks") with ordinality blue(name, ord), unnest("Team2Picks") with ordinality red(name2, ord)
    where blue.ord = red.ord
)

select * from unnested
  