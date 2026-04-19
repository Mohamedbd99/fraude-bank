from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FraudDetectionExport") \
    .getOrCreate()

schema = StructType([
    StructField("TransactionID", StringType()),
    StructField("AccountID", StringType()),
    StructField("TransactionAmount", DoubleType()),
    StructField("TransactionDate", StringType()),
    StructField("TransactionType", StringType()),
    StructField("Location", StringType()),
    StructField("DeviceID", StringType()),
    StructField("IPAddress", StringType()),
    StructField("MerchantID", StringType()),
    StructField("Channel", StringType()),
    StructField("CustomerAge", IntegerType()),
    StructField("CustomerOccupation", StringType()),
    StructField("TransactionDuration", IntegerType()),
    StructField("LoginAttempts", IntegerType()),
    StructField("AccountBalance", DoubleType()),
    StructField("PreviousTransactionDate", StringType()),
])

df = spark.read.csv(
    "hdfs://namenode:9000/data/sqoop_import/part-m-00000",
    header=False,
    schema=schema
)

print(f"Total transactions loaded: {df.count()}")

fraud_df = df.filter(
    (col("TransactionAmount") > 1000) |
    (col("LoginAttempts") > 3) |
    (col("TransactionDuration") > 200)
)

count = fraud_df.count()
print(f"Suspicious transactions detected: {count}")
fraud_df.show(10, truncate=False)

fraud_df.coalesce(1).write.mode("overwrite").option("header", "false").csv(
    "hdfs://namenode:9000/data/fraud_alerts"
)
print("Fraud results saved to HDFS: hdfs://namenode:9000/data/fraud_alerts")

spark.stop()
