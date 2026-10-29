with nat as (
  select drg_code, sum(total_discharges * avg_total_payment) / sum(total_discharges) as nat_avg_payment
  from {{ ref('fct_inpatient') }} group by 1)
select f.provider_ccn, p.provider_name, p.state, sum(f.total_discharges) as discharges,
       sum(f.total_discharges * f.avg_total_payment) / sum(f.total_discharges * n.nat_avg_payment) as payment_index
from {{ ref('fct_inpatient') }} f join nat n using (drg_code) join {{ ref('dim_provider') }} p using (provider_ccn)
group by 1, 2, 3 having sum(f.total_discharges) >= 500
