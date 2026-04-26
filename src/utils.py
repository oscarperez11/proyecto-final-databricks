"""Funciones utilitarias compartidas entre notebooks del ETL medallion."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def clean_column_names(df: DataFrame) -> DataFrame:
    """Normaliza nombres de columnas a snake_case minúsculas."""
    for col in df.columns:
        normalized = col.strip().lower().replace(" ", "_").replace("-", "_")
        if normalized != col:
            df = df.withColumnRenamed(col, normalized)
    return df


def add_audit_columns(df: DataFrame, layer: str) -> DataFrame:
    """Agrega columnas de auditoría estándar para cada capa."""
    return (
        df
        .withColumn(f"_{layer}_timestamp", F.current_timestamp())
        .withColumn(f"_{layer}_date", F.current_date())
    )


def configure_managed_identity(spark: SparkSession, storage_account: str) -> None:
    """Configura autenticación OAuth con Managed Identity para ADLS Gen2."""
    base = "fs.azure.account"
    fqdn = f"{storage_account}.dfs.core.windows.net"

    spark.conf.set(f"{base}.auth.type.{fqdn}", "OAuth")
    spark.conf.set(
        f"{base}.oauth.provider.type.{fqdn}",
        "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
    )
    spark.conf.set(
        f"{base}.oauth2.msi.endpoint.{fqdn}",
        "http://169.254.169.254/oauth2/token",
    )


def null_summary(df: DataFrame) -> DataFrame:
    """Devuelve un DataFrame con el conteo de nulos por columna."""
    null_counts = [
        F.sum(F.col(c).isNull().cast(T.IntegerType())).alias(c)
        for c in df.columns
    ]
    return df.select(null_counts)


def enforce_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    """Castea un DataFrame al esquema indicado, columna por columna."""
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


ECOMMERCE_SILVER_SCHEMA = T.StructType([
    T.StructField("order_id",          T.StringType(),  False),
    T.StructField("order_date",        T.DateType(),    True),
    T.StructField("ship_date",         T.DateType(),    True),
    T.StructField("ship_mode",         T.StringType(),  True),
    T.StructField("customer_id",       T.StringType(),  True),
    T.StructField("customer_name",     T.StringType(),  True),
    T.StructField("segment",           T.StringType(),  True),
    T.StructField("country",           T.StringType(),  True),
    T.StructField("city",              T.StringType(),  True),
    T.StructField("state",             T.StringType(),  True),
    T.StructField("region",            T.StringType(),  True),
    T.StructField("product_id",        T.StringType(),  True),
    T.StructField("category",          T.StringType(),  True),
    T.StructField("sub_category",      T.StringType(),  True),
    T.StructField("product_name",      T.StringType(),  True),
    T.StructField("sales",             T.DoubleType(),  True),
    T.StructField("quantity",          T.IntegerType(), True),
    T.StructField("discount",          T.DoubleType(),  True),
    T.StructField("profit",            T.DoubleType(),  True),
    T.StructField("revenue_net",       T.DoubleType(),  True),
    T.StructField("profit_margin_pct", T.DoubleType(),  True),
    T.StructField("order_year",        T.IntegerType(), True),
    T.StructField("order_month",       T.IntegerType(), True),
])

SUPERSTORE_SILVER_SCHEMA = T.StructType([
    T.StructField("row_id",           T.IntegerType(), True),
    T.StructField("order_id",         T.StringType(),  False),
    T.StructField("order_date",       T.DateType(),    True),
    T.StructField("ship_date",        T.DateType(),    True),
    T.StructField("ship_mode",        T.StringType(),  True),
    T.StructField("customer_id",      T.StringType(),  True),
    T.StructField("customer_name",    T.StringType(),  True),
    T.StructField("segment",          T.StringType(),  True),
    T.StructField("country",          T.StringType(),  True),
    T.StructField("city",             T.StringType(),  True),
    T.StructField("state",            T.StringType(),  True),
    T.StructField("postal_code",      T.StringType(),  True),
    T.StructField("region",           T.StringType(),  True),
    T.StructField("product_id",       T.StringType(),  True),
    T.StructField("category",         T.StringType(),  True),
    T.StructField("sub_category",     T.StringType(),  True),
    T.StructField("product_name",     T.StringType(),  True),
    T.StructField("sales",            T.DoubleType(),  True),
    T.StructField("quantity",         T.IntegerType(), True),
    T.StructField("discount",         T.DoubleType(),  True),
    T.StructField("profit",           T.DoubleType(),  True),
    T.StructField("profit_category",  T.StringType(),  True),
    T.StructField("days_to_ship",     T.IntegerType(), True),
    T.StructField("order_year",       T.IntegerType(), True),
    T.StructField("order_month",      T.IntegerType(), True),
])
