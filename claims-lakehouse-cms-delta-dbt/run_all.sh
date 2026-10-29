#!/usr/bin/env bash
# Usage: CL_LAKE=/some/dir ./run_all.sh   (put the two CMS CSVs in data_raw/ first)
set -e
export CL_LAKE=${CL_LAKE:-$PWD/lake} SPARK_LOCAL_IP=127.0.0.1
for ds in inpatient partd; do python src/bronze.py $ds; python src/silver.py $ds; done
(cd dbt_project && DBT_PROFILES_DIR=. dbt build)
python -m pytest tests -q
