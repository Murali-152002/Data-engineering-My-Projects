select provider_ccn, drg_code, count(*) n from {{ ref('fct_inpatient') }} group by 1, 2 having count(*) > 1
