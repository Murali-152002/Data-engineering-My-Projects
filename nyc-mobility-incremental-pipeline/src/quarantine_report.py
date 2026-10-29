"""What is actually in quarantine? Reasons overall, by month, and payment_type of negative-amount rows."""
from pyspark.sql import functions as F

from common import LAKE, log_metric, path, spark


def run(lake=None):
    sp = spark("qreport")
    q = sp.read.format("delta").load(path("quarantine", "trips", lake or LAKE))
    first = F.element_at(F.split("reasons_str", ","), 1)
    by_reason = {r[0]: r[1] for r in q.groupBy(first.alias("r")).count().orderBy(F.desc("count")).collect()}
    neg = q.filter(F.col("reasons_str").contains("negative_total_amount"))
    pay = {str(r[0]): r[1] for r in neg.groupBy("payment_type").count().orderBy(F.desc("count")).collect()}
    by_month = {r[0]: r[1] for r in q.filter(F.col("reasons_str").contains("negative_total_amount")).groupBy("file_month").count().orderBy("file_month").collect()}
    total = q.count()
    log_metric("metrics_quarantine", "report", total=total, by_primary_reason=by_reason, negative_total_amount_by_payment_type=pay,
               negative_total_amount_by_month=by_month)
    print("total", total); print(by_reason); print("neg by payment_type", pay); print("neg by month", by_month)


if __name__ == "__main__":
    run()
