select provider_ccn, drg_code, total_discharges, avg_submitted_charge, avg_total_payment, avg_medicare_payment,
       dq_flags, (dq_flags like '%payment_exceeds_charge%') as payment_exceeds_charge
from {{ ref('stg_inpatient') }}
