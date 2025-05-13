from pyspark.sql.functions import avg
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, sum, count, stddev
from pyspark.sql.functions import col, lit, when, date_format, current_date
import uuid
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType
from datetime import datetime
from DataIngestion1.SparkConfig import get_spark_session


def process_gold_data():

    spark = get_spark_session()

    salary_income_df = spark.read.format("delta").load("D:/data/delta/bronze/salary_income")
    debit_transactions_silver_df = spark.read.format("delta").load("D:/data/delta/silver/debit_transactions_silver")

    # 1. Clean and Prepare Debit Data
    debit_df = debit_transactions_silver_df \
        .withColumn("txn_date", to_date("txn_date")) \
        .withColumn("amount", col("amount").cast("double"))

    # 2. Average Salary per Customer
    avg_salary_df = salary_income_df.groupBy("customer_id") \
        .agg(avg("salary_amount").alias("avg_salary"))

    # 3. Aggregate Debit Features
    aggregated_df = debit_df.groupBy("customer_id").agg(
        sum("amount").alias("total_spend"),
        sum(when(col("category") == "Subscriptions", col("amount"))).alias("subs"),
        sum(when(col("category") == "Misc. Discretionary", col("amount"))).alias("misc"),
        sum(when(col("category") == "Rent", col("amount"))).alias("rent"),
        sum(when(col("category") == "Utilities", col("amount"))).alias("utilities"),
        sum(when(col("category") == "Credit Card Payment", col("amount"))).alias("credit"),
        count("*").alias("total_txns"),
        sum(when(col("category") == "Recurring", 1).otherwise(0)).alias("recurring_txns")
    )

    # 4. Calculate Volatility (stddev of daily spend)
    daily_spend_df = debit_df.groupBy("customer_id", "txn_date") \
        .agg(sum("amount").alias("daily_spend"))

    volatility_df = daily_spend_df.groupBy("customer_id") \
        .agg(stddev("daily_spend").alias("volatility_index"))

    # 5. Join All DataFrames
    summary_df = aggregated_df \
        .join(volatility_df, "customer_id", "left") \
        .join(avg_salary_df, "customer_id", "left") \
        .fillna(0)

    # 6. Derived Financial Ratios
    final_df = summary_df.withColumn(
        "discretionary_ratio",
        (col("subs") + col("misc")) / col("total_spend")
    ).withColumn(
        "fixed_commitment_ratio",
        (col("rent") + col("utilities") + col("credit")) / (col("avg_salary") * 3)
    ).withColumn(
        "salary_to_spend_ratio",
        (col("avg_salary") * 3) / col("total_spend")
    ).withColumn(
        "recurring_spend_ratio",
        col("recurring_txns") / col("total_txns")
    )

    final_df.select(
        "customer_id", "total_spend", "avg_salary", "subs", "misc", "rent", "utilities", "credit",
        "recurring_txns", "total_txns", "volatility_index",
        "discretionary_ratio", "fixed_commitment_ratio",
        "salary_to_spend_ratio", "recurring_spend_ratio"
    ).show(truncate=False)

    """
    Condition	                                    Logic                                   	Score Impact
    discretionary_ratio > 0.5	    Too much spent on non-essentials (subs + misc)	                -50
    fixed_commitment_ratio > 0.6	Too much fixed financial obligation vs salary × 3	            -100
    salary_to_spend_ratio < 1	    Spending more than income (unhealthy financials)	            -150
    volatility_index > 20000	    Highly inconsistent spending	                                -75
    recurring_spend_ratio < 0.2	    Too few recurring transactions (suggests financial chaos)      	-25
    """

    # 1. Scoring Logic — Apply conditions step by step
    scored_df = final_df.withColumn("base_score", lit(650)) \
        .withColumn("base_score", when(col("discretionary_ratio") > 0.5, col("base_score") - 50).otherwise(col("base_score"))) \
        .withColumn("base_score", when(col("fixed_commitment_ratio") > 0.6, col("base_score") - 100).otherwise(col("base_score"))) \
        .withColumn("base_score", when(col("salary_to_spend_ratio") < 1, col("base_score") - 150).otherwise(col("base_score"))) \
        .withColumn("base_score", when(col("volatility_index") > 20000, col("base_score") - 75).otherwise(col("base_score"))) \
        .withColumn("base_score", when(col("recurring_spend_ratio") < 0.2, col("base_score") - 25).otherwise(col("base_score"))) \
        .withColumn("sds_score", F.expr("least(900, greatest(0, base_score))")) \
        .drop("base_score")  # Optionally drop intermediate column

    scored_df = scored_df.withColumn("score_month", date_format(current_date(), "yyyy-MM"))

    final_score_df = scored_df.select(
        "customer_id",
        "total_spend",
        "discretionary_ratio",
        "fixed_commitment_ratio",
        "salary_to_spend_ratio",
        "volatility_index",
        "recurring_spend_ratio",
        "sds_score",
        "score_month"
    )

    final_score_df.show(truncate=False)
    final_score_df.write.format("delta").partitionBy("score_month").mode("overwrite").option("overwriteSchema", "true").save("D:/data/delta/gold/data_products_sds_financial_health_score")

    #  to maintain the audit log
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

    # Add UUID, timestamp, and user/system context
    audited_score_df = final_score_df.withColumn("calculation_id", generate_uuid()) \
        .withColumn("calculation_time", lit(datetime.utcnow().isoformat())) \
        .withColumn("calculated_by", lit("auto_pipeline"))  # or user/system id

    # Write audit log to Delta
    audited_score_df.write.format("delta").mode("append").save("D:/data/delta/audit_log/sds_score_audit")

if __name__ == "__main__":
    process_gold_data()