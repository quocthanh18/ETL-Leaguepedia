with source as (
      select * from {{ source('staging', 'scoreboardgames') }}
),
renamed as (
    select
        {{ adapter.quote("OverviewPage") }},
        {{ adapter.quote("Team1") }},
        {{ adapter.quote("Team2") }},
        {{ adapter.quote("WinTeam") }},
        {{ adapter.quote("DateTime UTC") }},
        {{ adapter.quote("Gamelength") }},
        {{ adapter.quote("GameId") }}
    from source
)
select * from renamed
  