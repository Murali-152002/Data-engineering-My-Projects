select p.specialty, count(*) as prescribers, sum(f.tot_clms) as total_claims, sum(f.tot_drug_cst) as total_drug_cost,
       sum(f.opioid_tot_clms) * 1.0 / nullif(sum(f.tot_clms), 0) as opioid_claim_share,
       sum(f.gnrc_tot_clms) * 1.0 / nullif(sum(f.gnrc_tot_clms + f.brnd_tot_clms), 0) as generic_claim_share
from {{ ref('fct_prescribing') }} f join {{ ref('dim_prescriber') }} p using (npi)
group by 1
