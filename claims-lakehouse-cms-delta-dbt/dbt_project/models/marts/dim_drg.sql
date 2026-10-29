select distinct drg_code, drg_desc from {{ ref('stg_inpatient') }}
