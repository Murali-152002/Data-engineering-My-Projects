select location_id, borough, zone, service_zone,
       service_zone = 'Airports' or service_zone = 'EWR' as is_airport
from {{ ref('stg_zones') }}
