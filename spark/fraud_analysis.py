from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/transactions.csv", header=True, inferSchema=True)

print("Schema:")
df.printSchema()

print("First rows:")
df.show(10, truncate=False)

fraud_df = df.filter(
    (col("TransactionAmount") > 1000) |
    (col("LoginAttempts") > 3) |
    (col("TransactionDuration") > 200)
)

print("Suspicious transactions:")
fraud_df.show(20, truncate=False)

print("Number of suspicious transactions:", fraud_df.count())

spark.stop()