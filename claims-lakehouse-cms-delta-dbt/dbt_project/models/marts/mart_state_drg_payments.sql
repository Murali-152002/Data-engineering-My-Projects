select f.drg_code, p.state, count(*) as hospitals, sum(f.total_discharges) as discharges,
       sum(f.total_discharges * f.avg_total_payment) / sum(f.total_discharges) as weighted_avg_total_payment
from {{ ref('fct_inpatient') }} f join {{ ref('dim_provider') }} p using (provider_ccn)
group by 1, 2
