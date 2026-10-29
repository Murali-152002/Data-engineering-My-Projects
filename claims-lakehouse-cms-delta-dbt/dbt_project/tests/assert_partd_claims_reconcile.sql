select 1 from (select (select sum(total_claims) from {{ ref('mart_specialty_prescribing') }}) a, (select sum(tot_clms) from {{ ref('fct_prescribing') }}) b) where a != b
