"""Tests unitarios para funciones de transformación del ETL."""

import sys
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from utils import clean_column_names, null_summary, add_audit_columns


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("etl_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


class TestCleanColumnNames:
    def test_spaces_replaced(self, spark):
        df = spark.createDataFrame([(1,)], ["Column With Spaces"])
        result = clean_column_names(df)
        assert "column_with_spaces" in result.columns

    def test_uppercase_lowered(self, spark):
        df = spark.createDataFrame([(1,)], ["OrderID"])
        result = clean_column_names(df)
        assert "orderid" in result.columns

    def test_hyphens_replaced(self, spark):
        df = spark.createDataFrame([(1,)], ["order-date"])
        result = clean_column_names(df)
        assert "order_date" in result.columns

    def test_already_clean_unchanged(self, spark):
        df = spark.createDataFrame([(1,)], ["order_id"])
        result = clean_column_names(df)
        assert result.columns == ["order_id"]


class TestNullSummary:
    def test_counts_nulls(self, spark):
        schema = T.StructType([
            T.StructField("sales",  T.DoubleType(), True),
            T.StructField("profit", T.DoubleType(), True),
        ])
        data = [(1.0, None), (None, 2.0), (3.0, 4.0)]
        df = spark.createDataFrame(data, schema)

        result = null_summary(df).collect()[0]
        assert result["sales"]  == 1
        assert result["profit"] == 1

    def test_no_nulls(self, spark):
        schema = T.StructType([T.StructField("val", T.IntegerType(), True)])
        df = spark.createDataFrame([(1,), (2,)], schema)
        result = null_summary(df).collect()[0]
        assert result["val"] == 0


class TestAddAuditColumns:
    def test_adds_timestamp_column(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_audit_columns(df, "bronze")
        assert "_bronze_timestamp" in result.columns

    def test_adds_date_column(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_audit_columns(df, "silver")
        assert "_silver_date" in result.columns


class TestSilverBusinessRules:
    def test_profit_margin_zero_when_no_sales(self, spark):
        schema = T.StructType([
            T.StructField("sales",  T.DoubleType(), True),
            T.StructField("profit", T.DoubleType(), True),
        ])
        df = spark.createDataFrame([(0.0, 10.0)], schema)
        result = df.withColumn(
            "profit_margin_pct",
            F.when(F.col("sales") > 0,
                   F.round((F.col("profit") / F.col("sales")) * 100, 2)
            ).otherwise(F.lit(0.0))
        )
        row = result.collect()[0]
        assert row["profit_margin_pct"] == 0.0

    def test_profit_category_loss(self, spark):
        schema = T.StructType([T.StructField("profit", T.DoubleType(), True)])
        df = spark.createDataFrame([(-5.0,)], schema)
        result = df.withColumn(
            "profit_category",
            F.when(F.col("profit") > 100, "Alto")
             .when(F.col("profit") > 0,   "Positivo")
             .when(F.col("profit") == 0,  "Neutro")
             .otherwise("Pérdida")
        )
        assert result.collect()[0]["profit_category"] == "Pérdida"

    def test_profit_category_alto(self, spark):
        schema = T.StructType([T.StructField("profit", T.DoubleType(), True)])
        df = spark.createDataFrame([(150.0,)], schema)
        result = df.withColumn(
            "profit_category",
            F.when(F.col("profit") > 100, "Alto")
             .when(F.col("profit") > 0,   "Positivo")
             .when(F.col("profit") == 0,  "Neutro")
             .otherwise("Pérdida")
        )
        assert result.collect()[0]["profit_category"] == "Alto"
