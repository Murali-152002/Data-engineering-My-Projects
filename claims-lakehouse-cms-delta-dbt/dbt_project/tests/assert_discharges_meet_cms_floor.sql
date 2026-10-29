select * from {{ ref('fct_inpatient') }} where total_discharges < 11
