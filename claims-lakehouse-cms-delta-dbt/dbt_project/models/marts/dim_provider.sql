select distinct provider_ccn, provider_name, city, state, state_fips, zip5, ruca from {{ ref('stg_inpatient') }}
