# CMS Claims Lakehouse (real public data)

Bronze / silver / gold pipeline over two real CMS Medicare public-use files, built with PySpark (local mode), Delta Lake and dbt on DuckDB. Everything below was measured on a real run (4-core, 4 GB Linux VM).

**Honest scope:** local run, no cloud deployment yet. Bronze, silver and quarantine are Delta tables. The dbt gold layer reads a Parquet snapshot exported from the Delta silver table (DuckDB cannot read Delta offline here). The CMS data are aggregated public files, not patient-level claims.

## Data
| Dataset | Source | Rows | Columns |
|---|---|---|---|
| Medicare Inpatient Hospitals, by Provider and Service (2024) | data.cms.gov | 145,879 | 15 |
| Medicare Part D Prescribers, by Provider (2024) | data.cms.gov | 1,416,883 | 84 |

## Pipeline
1. **Bronze** (`src/bronze.py`): land the CSV as-is (all strings) plus `_source_file` and `_ingested_at` as a Delta table. Inpatient 9.1 s, Part D 25.4 s.
2. **Silver** (`src/silver.py`): typed columns, trimmed strings, 8 hard rules for inpatient and 5 for Part D, soft flags, and a quarantine table with a reason per row. Silver is written with a Delta `MERGE` on the business key (provider+DRG, NPI), so re-runs are idempotent. Bronze rows must equal silver plus quarantine, or the job fails.
3. **Gold** (`dbt_project`): star schemas and marts in DuckDB.
   - Inpatient: `dim_provider`, `dim_drg`, `fct_inpatient`, `mart_state_drg_payments`, `mart_provider_cost_index`
   - Part D: `dim_prescriber`, `fct_prescribing`, `mart_specialty_prescribing`
4. **Tests**: 32 dbt data tests (keys, not-null, relationships, grain, reconciliation) and 6 pytest unit tests on the validation rules.

## CI
Every push runs unit tests, then bronze/silver/gold on a 1,500-row real-data sample of each file (`data_sample/`), a second silver run to prove the Delta MERGE is idempotent, and `dbt build`. See `.github/workflows/claims-lakehouse-ci.yml` at the repo root.

## Measured results
| Check | Result |
|---|---|
| Inpatient bronze to silver | 145,879 in, 145,879 valid, 0 quarantined, reconciled |
| Part D bronze to silver | 1,416,883 in, 1,416,881 valid, 2 quarantined (null key fields), reconciled |
| Soft flags | 223 inpatient rows where average payment exceeds average submitted charge; 41 Part D rows with claims but zero drug cost |
| Idempotent re-run | Second silver run used `MERGE`: silver stayed 145,879 (inpatient) and 1,416,881 (Part D) rows; Delta history shows v0 WRITE, v1 MERGE |
| Duplicates on business key | 0 (inpatient provider+DRG, Part D NPI) |
| dbt build (both datasets) | 8 table models, 2 views, 32 tests, 42 of 42 passed |
| pytest | 6 of 6 passed |

The CMS files are already clean, so the rules mostly confirm quality. The useful findings are the flagged rows and the marts below.

## What the marts show
- 2,906 hospitals, 540 DRGs, 4,952,481 Medicare discharges in the inpatient file.
- DRG 470 (major hip/knee joint replacement without MCC): the discharge-weighted average payment ranges from $11,943 (VT) to $25,079 (MD) across states, about 2.1x.
- Part D: 1.71 billion claims and $288.4 billion drug cost across 214 specialties. Highest opioid claim share among specialties with 500+ prescribers: Interventional Pain Management 55.7%, Pain Management 52.1%, Anesthesiology 49.3%.
- Sample outputs: `results/gold_samples/`. Raw run metrics: `results/metrics_*.json`.

## Run it
Download both CSVs from data.cms.gov into `data_raw/` and put `delta-spark_2.12-3.2.0.jar` and `delta-storage-3.2.0.jar` (Maven Central) in `jars/`, then `pip install -r requirements.txt && ./run_all.sh`.
