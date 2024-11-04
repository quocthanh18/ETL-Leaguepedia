with source as (
      select * from {{ source('staging', 'tournamentresults') }}
),
renamed as (
    select
        {{ adapter.quote("OverviewPage") }},
        {{ adapter.quote("Prize USD") }},
        {{ adapter.quote("Place") }},
        {{ adapter.quote("Team") }}

    from source
)
select * from renamed
  