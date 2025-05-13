from pyspark.sql.types import DoubleType
from DataIngestion1.SparkConfig import get_spark_session
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import col, count, year, month

def process_silver_data():
    spark = get_spark_session()

    debit_transactions_silver_df = spark.read.format("delta").load("D:/data/delta/bronze/debit_transactions")

    # Calculate spend per category over 3-month period
    window_spec = Window.partitionBy("customer_id", "category") \
        .rowsBetween(-2, 0)  # rolling window of current + past 2 months

    three_month_spend_df = debit_transactions_silver_df.withColumn(
        "three_month_category_spend",
        F.sum("amount").over(window_spec)
    )
    three_month_spend_df.show(10)

    # Compute total spend across all categories
    window_spec = Window.partitionBy("customer_id")
    total_spend = debit_transactions_silver_df.withColumn(
        "total_spend",
        F.sum("amount").over(window_spec)
    )
    total_spend.show(10)

    # Aggregate transaction dates to compute volatility (daily stddev)
    debit_transactions_silver_df = debit_transactions_silver_df.withColumn("amount", F.col("amount").cast(DoubleType()))

    # Aggregate daily spend per customer
    daily_spend_df = debit_transactions_silver_df.groupBy("customer_id", "txn_date") \
        .agg(F.sum("amount").alias("daily_spend"))

    # Compute volatility (stddev of daily spend per customer)
    volatility_df = daily_spend_df.groupBy("customer_id") \
        .agg(F.stddev("daily_spend").alias("spend_volatility"))
    volatility_df.show(10)

    # Identify recurring transactions (duplicates in category + merchant)
    # Group by customer_id, category, and merchant
    recurring_transactions_df = debit_transactions_silver_df.groupBy("customer_id", "category", "merchant") \
        .agg(count("txn_type").alias("transaction_count"))
    # Filter recurring transactions (count > 1)
    recurring_transactions_df = recurring_transactions_df.filter(col("transaction_count") > 1)
    recurring_transactions_df.show(10)

    aggregated_df = total_spend \
        .join(three_month_spend_df, on="customer_id", how="left") \
        .join(volatility_df, on="customer_id", how="left") \
        .join(recurring_transactions_df, on="customer_id", how="left")
    aggregated_df.show(10)

    # Aggregate Monthly Spend Metrics
    # Group and pivot transactions by customer_id
    debit_transactions_silver_df = debit_transactions_silver_df \
        .withColumn("year", year(col("txn_date"))) \
        .withColumn("month", month(col("txn_date")))

    monthly_spend_df = debit_transactions_silver_df.groupBy(
        "customer_id", "merchant", "category", "year", "month"
    ).agg(
        F.sum("amount").alias("monthly_spend")
    )

    monthly_spend_df = monthly_spend_df.withColumn(
        "year_month",
        F.concat_ws("_", F.col("year"), F.date_format(F.to_date(F.col("month").cast("string"), "M"), "MMM"))
    )

    pivot_df = monthly_spend_df.groupBy("customer_id") \
        .pivot("year_month") \
        .agg(F.first("monthly_spend"))

    pivot_df.show(10)

    debit_transactions_silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("D:/data/delta/silver/debit_transactions_silver")

if __name__ == "__main__":
    process_silver_data()