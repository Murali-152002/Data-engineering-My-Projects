select * from {{ ref('fct_inpatient') }} where avg_medicare_payment > avg_total_payment
