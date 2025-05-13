import seaborn as sns
from pyspark.sql.functions import sum as spark_sum
import matplotlib.pyplot as plt
from DataIngestion1.SparkConfig import get_spark_session

def visualize_gold_data():
    spark = get_spark_session()

    df = spark.read.format("delta").load("D:/data/delta/gold/data_products_sds_financial_health_score")

    pdf = df.toPandas()

    # ---- 1. SDS Score Distribution
    sds_count = pdf['sds_score'].value_counts().sort_index()
    plt.figure(figsize=(6,4))
    sds_count.plot(kind='bar', color='teal')
    plt.title("SDS Score Distribution")
    plt.xlabel("SDS Score")
    plt.ylabel("Number of Customers")
    plt.tight_layout()
    plt.show()

    # ---- 2. Discretionary Ratio Distribution
    discretionary_count = pdf['discretionary_ratio'].value_counts()
    plt.figure(figsize=(6,4))
    discretionary_count.plot(kind='bar', color='orange')
    plt.title("Discretionary Ratio Distribution")
    plt.xticks(ticks=[0, 1], labels=["Non-Discretionary (0)", "Discretionary (1)"])
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()

    # ---- 3. Total Spend vs SDS Score
    plt.figure(figsize=(8,6))
    sns.boxplot(data=pdf, x="sds_score", y="total_spend")
    plt.title("Total Spend Distribution by SDS Score")
    plt.tight_layout()
    plt.show()

    # ---- 4. Salary to Spend Ratio vs SDS Score
    plt.figure(figsize=(8,6))
    sns.scatterplot(data=pdf, x="salary_to_spend_ratio", y="sds_score", hue="discretionary_ratio", palette="coolwarm")
    plt.title("Salary to Spend Ratio vs SDS Score")
    plt.tight_layout()
    plt.show()

    # ---- 5. Total Spend % by Discretionary Type
    total_spend_group = df.groupBy("discretionary_ratio").agg(spark_sum("total_spend").alias("total_spend")).toPandas()
    plt.figure(figsize=(6,6))
    plt.pie(
        total_spend_group["total_spend"],
        labels=["Non-Discretionary", "Discretionary"] if 0 in total_spend_group["discretionary_ratio"].values else ["Discretionary", "Non-Discretionary"],
        autopct="%1.1f%%",
        startangle=140,
        colors=["lightcoral", "lightskyblue"]
    )
    plt.title("Total Spend Share by Discretionary Ratio")
    plt.tight_layout()
    plt.show()

    # ---- 6. Top 5 Spenders
    top5 = pdf.sort_values(by="total_spend", ascending=False).head(5)
    plt.figure(figsize=(8,5))
    sns.barplot(data=top5, x="customer_id", y="total_spend", hue="sds_score", palette="Set2")
    plt.title("Top 5 Customers by Spend with SDS Scores")
    plt.tight_layout()
    plt.show()

    # ---- 7. Heatmap: Correlation
    plt.figure(figsize=(10,6))
    sns.heatmap(pdf[["total_spend", "discretionary_ratio", "salary_to_spend_ratio", "sds_score"]].corr(), annot=True, cmap="YlGnBu")
    plt.title("Correlation between Financial Metrics")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    visualize_gold_data()