from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder \
        .appName("PostgreSQL and Delta Integration") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5,io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "false") \
        .getOrCreate()
    return spark