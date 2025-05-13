import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col,lower,when
from pyspark.sql.functions import current_date
from DataIngestion1.SparkConfig import get_spark_session

def process_bronze_data():

    spark = get_spark_session()

    # Load configuration
    with open("../DataIngestion/db_config.json", "r") as config_file:
        config = json.load(config_file)

    # Read salary_income table
    salary_income_df = spark.read \
        .format("jdbc") \
        .option("url", config["url"]) \
        .option("dbtable", "salary_income") \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", config["driver"]) \
        .load()

    # Read transactions table
    transactions_df = spark.read \
        .format("jdbc") \
        .option("url", config["url"]) \
        .option("dbtable", "transactions") \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", config["driver"]) \
        .load()

    subscriptions_df = spark.read.parquet(r"file:///D:\data\delta\subscriptions.parquet")

    credit_card_statements_df = spark.read.csv(
        "D:/data/delta/credit_card_statements.csv",
        header=True,
        inferSchema=True
    )

    salary_income_df = salary_income_df.select(
        col("customer_id").cast("int"),
        col("salary_amount").cast("double"),
        col("pay_date").cast("date"),
        current_date().alias("ingestion_date")
    )

    transactions_df = transactions_df.select(
        col("customer_id").cast("int"),
        col("txn_date").cast("date"),
        col("amount").cast("double"),
        lower(col("txn_type")).alias("txn_type"),
        lower(col("merchant")).alias("merchant"),
        lower(col("channel")).alias("channel"),
        col("mcc_code").cast("int"),
        current_date().alias("ingestion_date")
    )

    subscriptions_df = subscriptions_df.select(
        col("customer_id").cast("long"),
        col("service_name").cast("String"),
        col("amount").cast("double"),
        col("frequency").cast("String"),
        current_date().alias("ingestion_date")
    )

    # Credit Card Statements
    credit_card_statements_df = credit_card_statements_df.select(
        col("customer_id").cast("int"),
        col("statement_date").cast("date"),
        col("total_due").cast("double"),
        col("minimum_due").cast("double"),
        col("paid_amount").cast("int"),
        current_date().alias("ingestion_date")
    )


    debit_transactions_df = transactions_df.filter(lower(col("txn_type")) == "debit")

    debit_transactions_df = (debit_transactions_df
              .withColumn("category",
               when(F.col("merchant").like("%zillow%") | F.col("merchant").like("%rentpay%") | F.col("merchant").like("%landlord%"), "Rent")
              .when(F.col("merchant").like("%hydro%") | F.col("merchant").like("%enbridge%") | F.col("merchant").like("%telus%"), "Utilities")
              .when(F.col("merchant").like("%netflix%") | F.col("merchant").like("%spotify%") | F.col("merchant").like("%amazon prime%"), "Subscriptions")
              .when(F.col("merchant").like("%zomato%") | F.col("merchant").like("%costco%") | F.col("merchant").like("%uber eats%") | F.col("merchant").like("%uber%") | F.col("merchant").like("%swiggy%"), "Food & Groceries")
              .when(F.col("merchant").like("%visa%") | F.col("merchant").like("%amex%") | F.col("merchant").like("%emi%"), "Credit Repayment")
              .when(F.col("merchant").like("%amazon%") | F.col("merchant").like("%shopping%") | F.col("merchant").like("%flipkart%") | F.col("merchant").like("%uber%") | F.col("merchant").like("%bigbazaar%") | F.col("merchant").like("%myntra%") | F.col("merchant").like("%flights%"), "Misc. Discretionary")
              .otherwise("Other")))

    salary_income_df.write.format("delta").mode("append").partitionBy("ingestion_date").save("D:/data/delta/bronze/salary_income")
    transactions_df.write.format("delta").mode("append").partitionBy("ingestion_date").save("D:/data/delta/bronze/transactions")
    debit_transactions_df.write.format("delta").mode("append").partitionBy("ingestion_date").save("D:/data/delta/bronze/debit_transactions")
    subscriptions_df.write.format("delta").mode("append").partitionBy("ingestion_date").save("D:/data/delta/bronze/subscriptions")
    credit_card_statements_df.write.format("delta").mode("append").partitionBy("ingestion_date").save("D:/data/delta/bronze/credit_card_statements")

if __name__ == "__main__":
    process_bronze_data()

"""
# nlp based on the category, it help for the large scale messy data, to match applying on the fuzzy data
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

# Example training data
category_examples = {
    "Rent": ["Zillow", "RentPay", "Landlord"],
    "Utilities": ["BC Hydro", "Enbridge", "Telus"],
    "Subscriptions": ["Netflix", "Spotify", "Amazon Prime"],
    "Food & Groceries": ["Zomato", "Costco", "Uber Eats"],
    "Credit Repayment": ["Visa", "AMEX", "EMI Portal"],
    "Misc. Discretionary": ["Amazon", "Shopping", "Flights"],
}

# Flatten training data
training_data = []
for category, merchants in category_examples.items():
    for merchant in merchants:
        training_data.append({"merchant": merchant.lower(), "category": category})
training_df = pd.DataFrame(training_data)

# Vectorize known merchants
vectorizer = TfidfVectorizer()
X_train = vectorizer.fit_transform(training_df["merchant"])

def predict_category(merchant_name):
    merchant_vec = vectorizer.transform([merchant_name.lower()])
    sim = cosine_similarity(merchant_vec, X_train)
    best_match_idx = sim.argmax()
    return training_df.iloc[best_match_idx]["category"]

# Example usage:
predict_category("Enbrdge")  # → Utilities (even with typo)
"""