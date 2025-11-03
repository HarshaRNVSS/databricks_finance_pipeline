# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# 1️⃣ Stream read from Bronze Unity Catalog table
@dlt.view(
    name="bronze_stream",
    comment="Streaming read from Bronze table in Unity Catalog"
)
def bronze_stream():
    return spark.readStream.table("finance.bronze.trades_bronze")

# 2️⃣ Clean & transform data
@dlt.table(
    name="trades_clean",
    comment="Parsed trade data with derived fields",
    table_properties={"quality": "silver"}
)
def trades_clean():
    return (
        dlt.read_stream("bronze_stream")
            .filter(col("symbol").isNotNull())
            .withColumn("trade_value", col("price") * col("quantity"))
            .withColumn("trade_time", col("trade_time").cast("timestamp"))
            .withColumn("hour", hour("trade_time"))
            .withColumn("day", dayofmonth("trade_time"))
            .withColumn("month", month("trade_time"))
            .withColumn("year", year("trade_time"))
    )

# 3️⃣ Apply data quality checks
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_trade_time", "trade_time IS NOT NULL")
@dlt.table(
    name="trades_validated",
    comment="Validated trade records",
    table_properties={"quality": "silver"}
)
def trades_validated():
    return dlt.read("trades_clean")

# 4️⃣ Dimension tables
@dlt.table(
    name="dim_symbol",
    comment="Unique symbols dimension",
    table_properties={"quality": "silver"}
)
def dim_symbol():
    df = dlt.read("trades_validated")
    return (
        df.select("symbol")
          .distinct()
          .withColumn("symbol_id", monotonically_increasing_id())
    )

@dlt.table(
    name="dim_time",
    comment="Time dimension for trade events",
    table_properties={"quality": "silver"}
)
def dim_time():
    df = dlt.read("trades_validated")
    return (
        df.select(col("trade_time").alias("datetime"))
          .withColumn("hour", hour("datetime"))
          .withColumn("day", dayofmonth("datetime"))
          .withColumn("month", month("datetime"))
          .withColumn("year", year("datetime"))
          .distinct()
    )