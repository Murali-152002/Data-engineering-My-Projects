"""Silver: typed, validated, de-duplicated. Hard failures go to quarantine with a reason; soft anomalies stay with a flag."""
import sys, time
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window
from common import *
ds = sys.argv[1]; t0 = time.time()
sp = spark(f"silver_{ds}")
b = sp.read.format("delta").load(os.path.join(LAKE, "bronze", ds))
nb = b.count()
def trim_all(df):
    for c, t in df.dtypes:
        if t == "string" and not c.startswith("_"):
            df = df.withColumn(c, F.trim(F.col(c)))
    return df
b = trim_all(b)

if ds == "inpatient":
    s = (b.select(
        F.col("Rndrng_Prvdr_CCN").alias("provider_ccn"), F.col("Rndrng_Prvdr_Org_Name").alias("provider_name"),
        F.col("Rndrng_Prvdr_City").alias("city"), F.col("Rndrng_Prvdr_State_Abrvtn").alias("state"),
        F.col("Rndrng_Prvdr_State_FIPS").alias("state_fips"), F.col("Rndrng_Prvdr_Zip5").alias("zip5"),
        F.col("Rndrng_Prvdr_RUCA").alias("ruca"), F.col("DRG_Cd").alias("drg_code"), F.col("DRG_Desc").alias("drg_desc"),
        F.col("Tot_Dschrgs").cast("int").alias("total_discharges"),
        F.col("Avg_Submtd_Cvrd_Chrg").cast("double").alias("avg_submitted_charge"),
        F.col("Avg_Tot_Pymt_Amt").cast("double").alias("avg_total_payment"),
        F.col("Avg_Mdcr_Pymt_Amt").cast("double").alias("avg_medicare_payment"),
        "_source_file", "_ingested_at"))
    w = Window.partitionBy("provider_ccn", "drg_code").orderBy(F.col("_ingested_at").desc())
    s = s.withColumn("_rn", F.row_number().over(w))
    rules = [
        ("null_key", F.col("provider_ccn").isNull() | F.col("drg_code").isNull()),
        ("bad_ccn_format", ~F.col("provider_ccn").rlike(r"^[0-9A-Z]{6}$")),
        ("bad_zip_format", ~F.col("zip5").rlike(r"^[0-9]{5}$")),
        ("non_numeric_metric", F.col("total_discharges").isNull() | F.col("avg_submitted_charge").isNull() | F.col("avg_total_payment").isNull() | F.col("avg_medicare_payment").isNull()),
        ("negative_value", (F.col("total_discharges") < 0) | (F.col("avg_total_payment") < 0) | (F.col("avg_medicare_payment") < 0) | (F.col("avg_submitted_charge") < 0)),
        ("medicare_exceeds_total_payment", F.col("avg_medicare_payment") > F.col("avg_total_payment")),
        ("below_cms_suppression_floor", F.col("total_discharges") < 11),
        ("duplicate_provider_drg", F.col("_rn") > 1),
    ]
    soft = [("payment_exceeds_charge", F.col("avg_total_payment") > F.col("avg_submitted_charge"))]
else:
    num_i = ["Tot_Clms", "Tot_30day_Fills", "Tot_Day_Suply", "Tot_Benes", "Opioid_Tot_Clms", "Opioid_Tot_Benes", "Brnd_Tot_Clms", "Gnrc_Tot_Clms", "Antbtc_Tot_Clms"]
    num_d = ["Tot_Drug_Cst", "Opioid_Tot_Drug_Cst", "Brnd_Tot_Drug_Cst", "Gnrc_Tot_Drug_Cst", "Opioid_Prscrbr_Rate", "Bene_Avg_Age", "Bene_Avg_Risk_Scre"]
    cols = [F.col("Prscrbr_NPI").alias("npi"), F.col("Prscrbr_Last_Org_Name").alias("last_or_org_name"), F.col("Prscrbr_First_Name").alias("first_name"),
            F.col("Prscrbr_Ent_Cd").alias("entity_code"), F.col("Prscrbr_City").alias("city"), F.col("Prscrbr_State_Abrvtn").alias("state"),
            F.col("Prscrbr_Zip5").alias("zip5"), F.col("Prscrbr_Type").alias("specialty")]
    cols += [F.col(c).cast("long").alias(c.lower()) for c in num_i] + [F.col(c).cast("double").alias(c.lower()) for c in num_d]
    cols += ["_source_file", "_ingested_at"]
    s = b.select(*cols)
    w = Window.partitionBy("npi").orderBy(F.col("_ingested_at").desc())
    s = s.withColumn("_rn", F.row_number().over(w))
    rules = [
        ("null_key", F.col("npi").isNull() | F.col("specialty").isNull()),
        ("bad_npi_format", ~F.col("npi").rlike(r"^[0-9]{10}$")),
        ("negative_value", (F.col("tot_clms") < 0) | (F.col("tot_drug_cst") < 0) | (F.col("tot_benes") < 0)),
        ("opioid_claims_exceed_total_claims", F.col("opioid_tot_clms") > F.col("tot_clms")),
        ("duplicate_npi", F.col("_rn") > 1),
    ]
    soft = [("opioid_rate_out_of_range", (F.col("opioid_prscrbr_rate") < 0) | (F.col("opioid_prscrbr_rate") > 100)),
            ("zero_cost_with_claims", (F.col("tot_drug_cst") == 0) & (F.col("tot_clms") > 0))]

reason = F.concat_ws(";", *[F.when(c, F.lit(n)) for n, c in rules])
s = s.withColumn("_dq_hard", reason)
sflags = F.concat_ws(";", *[F.when(c, F.lit(n)) for n, c in soft])
s = s.withColumn("dq_flags", sflags)
good = s.filter(F.col("_dq_hard") == "").drop("_dq_hard", "_rn")
bad = s.filter(F.col("_dq_hard") != "").withColumnRenamed("_dq_hard", "quarantine_reason").drop("_rn")
silver_path = os.path.join(LAKE, "silver", ds)
KEY = ["provider_ccn", "drg_code"] if ds == "inpatient" else ["npi"]
if DeltaTable.isDeltaTable(sp, silver_path):   # re-run: idempotent MERGE upsert on the business key
    cond = " AND ".join(f"t.{k} = s.{k}" for k in KEY)
    (DeltaTable.forPath(sp, silver_path).alias("t").merge(good.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    mode = "merge"
else:
    good.write.format("delta").save(silver_path); mode = "create"
bad.write.format("delta").mode("overwrite").save(os.path.join(LAKE, "quarantine", ds))
sg = sp.read.format("delta").load(silver_path); ng = sg.count()
nq = sp.read.format("delta").load(os.path.join(LAKE, "quarantine", ds)).count()
# snapshot for DuckDB/dbt (gold reads this export of the Delta silver table)
sg.write.mode("overwrite").parquet(os.path.join(LAKE, "silver_export", ds))
hist = [dict(version=r["version"], operation=r["operation"]) for r in DeltaTable.forPath(sp, silver_path).history().select("version", "operation").collect()]
rule_counts = {n: s.filter(c).count() for n, c in rules}
flag_counts = {n: sg.filter(F.col("dq_flags").contains(n)).count() for n, _ in soft}
assert nb == ng + nq, f"reconciliation failed {nb} != {ng}+{nq}"
log_metric(ds, "silver", bronze_rows=nb, silver_rows=ng, quarantined_rows=nq, hard_rule_hits=rule_counts, soft_flag_rows=flag_counts,
           reconciled=True, write_mode=mode, silver_delta_history=hist, seconds=round(time.time() - t0, 1))
print(ds, "bronze", nb, "silver", ng, "quarantine", nq, "hard", rule_counts, "soft", flag_counts, "sec", round(time.time() - t0, 1))
