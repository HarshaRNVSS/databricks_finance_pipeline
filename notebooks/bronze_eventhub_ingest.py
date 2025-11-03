# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1️⃣ Get Event Hub connection string securely
connection_string = dbutils.secrets.get(scope="eventhub_scope", key="eventhub_conn")

# 2️⃣ Configure Event Hub connection
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
}

# 3️⃣ Define schema for Binance trade events
trade_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True),
    StructField("buyer_maker", BooleanType(), True)
])

# 4️⃣ Read Event Hub stream
raw_df = (
    spark.readStream
         .format("eventhubs")
         .options(**eh_conf)
         .load()
)

# 5️⃣ Parse JSON payload
parsed_df = (
    raw_df
    .withColumn("json_data", from_json(col("body").cast("string"), trade_schema))
    .select("json_data.*", col("enqueuedTime").alias("event_ingest_time"))
    .withColumn("trade_time", (col("trade_time") / 1000).cast("timestamp"))
)

# 6️⃣ Write to Unity Catalog Bronze table (linked to ADLS)
(
    parsed_df.writeStream
        .format("delta")
        .option("checkpointLocation", "abfss://chkpt@adbsdatalake.dfs.core.windows.net/bronze_trades_checkpoint")
        .outputMode("append")
        .table("finance.bronze.trades_bronze")
)