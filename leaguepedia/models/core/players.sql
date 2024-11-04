with source as (
      select * from {{ source('staging', 'players') }}
),
renamed as (
    select
        {{ adapter.quote("Player") }},
        {{ adapter.quote("Country") }},
        {{ adapter.quote("Birthdate") }},
        {{ adapter.quote("Team") }},
        {{ adapter.quote("Residency") }},
        {{ adapter.quote("Role") }},
        {{ adapter.quote("RoleLast") }},
        {{ adapter.quote("IsRetired") }},
        {{ adapter.quote("ToWildrift") }},
        {{ adapter.quote("AllName") }}
    from source
)
select * from renamed
  