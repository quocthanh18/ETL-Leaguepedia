with source as (
      select * from {{ source('staging', 'scoreboardgames') }}
),
renamed as (
    select
        {{ adapter.quote("Team1Players") }},
        {{ adapter.quote("Team2Players") }},
        {{ adapter.quote("GameId") }}
    from source
),

temp as (
    select
        {{ adapter.quote("GameId") }},
        ARRAY[string_to_array("Team1Players", ',')]::text[] as "Team1Players",
        ARRAY[string_to_array("Team2Players", ',')]::text[] as "Team2Players"
    from renamed
),

unnested as (
    select "GameId", blue.name as "blue", red.name2 as "red", {{map_role('blue.ord')}} as "role"
    from temp, unnest("Team1Players") with ordinality blue(name, ord), unnest("Team2Players") with ordinality red(name2, ord)
    where blue.ord = red.ord
)

select * from unnested
  