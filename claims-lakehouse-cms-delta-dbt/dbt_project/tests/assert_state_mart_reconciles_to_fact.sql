select 1 from (select (select sum(discharges) from {{ ref('mart_state_drg_payments') }}) a, (select sum(total_discharges) from {{ ref('fct_inpatient') }}) b) where a != b
