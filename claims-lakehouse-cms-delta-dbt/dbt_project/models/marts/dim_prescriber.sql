select npi, last_or_org_name, first_name, entity_code, city, state, zip5, specialty from {{ ref('stg_partd') }}
