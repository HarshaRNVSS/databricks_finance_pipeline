# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.view(name="trades_validated_view")
def trades_validated_view():
    return dlt.read("finance.silver.trades_validated")

@dlt.view(name="dim_symbol_view")
def dim_symbol_view():
    return dlt.read("finance.silver.dim_symbol")

@dlt.view(name="dim_time_view")
def dim_time_view():
    return dlt.read("finance.silver.dim_time")

@dlt.table(
    name="fact_trades",
    comment="Fact table combining validated trades and dimensions",
    table_properties={"quality": "gold"}
)
def fact_trades():
    trades = dlt.read("trades_validated_view")
    dim_symbol = dlt.read("dim_symbol_view")
    dim_time = dlt.read("dim_time_view")

    joined = (
        trades.alias("t")
        .join(dim_symbol.alias("s"), col("t.symbol") == col("s.symbol"), "left")
        .join(dim_time.alias("dt"), col("t.trade_time") == col("dt.datetime"), "left")
    )

    return joined.select(
        col("s.symbol_id"),
        col("dt.datetime").alias("trade_datetime"),
        col("t.price"),
        col("t.quantity"),
        col("t.trade_value"),
        col("t.buyer_maker"),
        col("dt.year"),
        col("dt.month"),
        col("dt.day"),
        col("dt.hour")
    )

@dlt.table(
    name="agg_trade_metrics",
    comment="Aggregated KPIs per symbol and hour",
    table_properties={"quality": "gold"}
)
def agg_trade_metrics():
    df = dlt.read("fact_trades")
    return (
        df.groupBy("symbol_id", "year", "month", "day", "hour")
          .agg(
              count("*").alias("trade_count"),
              avg("price").alias("avg_price"),
              sum("trade_value").alias("total_volume"),
              max("price").alias("max_price"),
              min("price").alias("min_price")
          )
    )