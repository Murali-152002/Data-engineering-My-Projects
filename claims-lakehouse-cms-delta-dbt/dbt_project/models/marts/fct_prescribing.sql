select npi, tot_clms, tot_30day_fills, tot_drug_cst, tot_benes, opioid_tot_clms, opioid_tot_drug_cst,
       brnd_tot_clms, gnrc_tot_clms, brnd_tot_drug_cst, gnrc_tot_drug_cst, antbtc_tot_clms, dq_flags
from {{ ref('stg_partd') }}
