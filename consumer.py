from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import *
from datetime import datetime, timedelta
from hdfs3 import HDFileSystem
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Kafka consumer settings
# the following are just examples 
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'log_analytics'
kafka_group_id = 'spark-streaming-consumer-group'

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumerSparkStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest",
}

# Read the Kafka stream using spark.readStream
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Convert the binary message value to string
df = df.withColumn("value", col("value").cast("string"))

# neede to update here 
df = df.withColumn(
    "ip_address",
    regexp_extract(col("value"), r'^([\d.]+)', 1)
).withColumn(
    "timestamp",
    regexp_extract(col("value"), r'\[([^:]+)', 1)
).withColumn(
    "year",
    regexp_extract(col("timestamp"), r'(\d{4})', 1)
).withColumn(
    "month",
    regexp_extract(col("timestamp"), r'\d{4}-(\d{2})', 1)
).withColumn(
    "day",
    regexp_extract(col("timestamp"), r'(\d{4}-\d{2}-\d{2})', 1)
).withColumn(
    "hour",
    regexp_extract(col("timestamp"), r'(\d{2}):', 1)
).withColumn(
    "minute",
    regexp_extract(col("timestamp"), r':(\d{2})', 1)
).withColumn(
    "method",
    regexp_extract(col("timestamp"), r'\"(\w+)', 1)
).withColumn(
    "endpoint",
    regexp_extract(col("timestamp"), r'\"\s+(\S+)', 1)
).withColumn(
    "http_version",
    regexp_extract(col("timestamp"), r'\".*\"\s+(\S+)', 1)
).withColumn(
    "response_code",
    regexp_extract(col("timestamp"), r'\"\s+(\d{3})', 1)
).withColumn(
    "bytes",
    regexp_extract(col("timestamp"), r'(\d+)$', 1)
)


# Select the desired columns
parsed_df = df.select("ip_address", "timestamp", "year", "month", "day", "hour", "minute", "method", "endpoint", "http_version", "response_code", "bytes")

# Process the Kafka messages and write to console and text file
query = parsed_df.writeStream.outputMode("append").format("console").start()

# Set the current time as the start time
start_time = datetime.now()

# and also u can also use trigger method
while query.isActive:
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=10):
        query.stop()

local_path = '/Users/hannalee/Downloads/log-analytics/output' # an example

w_query = parsed_df.writeStream.format("parquet").outputMode("append").option("checkpointLocation", '/tmp/output/ch').option("path", local_path).start()

hdfs = HDFileSystem('localhost', port=9000)  

# Store Parquet files in HDFS
hdfs.put('output.parquet', local_path)

# Close the HDFS connection
hdfs.disconnect()