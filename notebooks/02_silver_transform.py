# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Silver — Limpieza y Transformación
# MAGIC
# MAGIC Lee desde Bronze, aplica limpieza, normalización de tipos y reglas de negocio.
# MAGIC Escribe en tablas Delta de la capa Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "etl_medallion", "Unity Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md ## 1. Silver — E-commerce Sales

# COMMAND ----------

df_ecom_raw = spark.table(f"{catalog}.bronze.ecommerce_sales")
df_ecom_raw.printSchema()

# COMMAND ----------

def clean_column_names(df: DataFrame) -> DataFrame:
    """Normaliza nombres de columnas: minúsculas, espacios -> guión bajo."""
    renamed = {c: c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns}
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df

# COMMAND ----------

df_ecom = (
    df_ecom_raw
    .transform(clean_column_names)
    # Eliminar filas sin identificador de pedido
    .filter(F.col("order_id").isNotNull())
    # Eliminar duplicados por pedido + producto
    .dropDuplicates(["order_id", "product_name"])
    # Tipos correctos
    .withColumn("order_date",  F.to_date(F.col("order_date"),  "M/d/yyyy"))
    .withColumn("ship_date",   F.to_date(F.col("ship_date"),   "M/d/yyyy"))
    .withColumn("sales",       F.col("sales").cast(T.DoubleType()))
    .withColumn("quantity",    F.col("quantity").cast(T.IntegerType()))
    .withColumn("discount",    F.col("discount").cast(T.DoubleType()))
    .withColumn("profit",      F.col("profit").cast(T.DoubleType()))
    # Reemplazar nulos en numéricas con 0
    .fillna({"sales": 0.0, "quantity": 0, "discount": 0.0, "profit": 0.0})
    # Columnas derivadas
    .withColumn("revenue_net",      F.round(F.col("sales") * (1 - F.col("discount")), 2))
    .withColumn("profit_margin_pct",
        F.when(F.col("sales") > 0,
               F.round((F.col("profit") / F.col("sales")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    # Auditoría
    .withColumn("_silver_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    df_ecom.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.ecommerce_sales")
)

count = spark.table(f"{catalog}.silver.ecommerce_sales").count()
print(f"[OK] {catalog}.silver.ecommerce_sales -> {count:,} filas")

# COMMAND ----------

# MAGIC %md ## 2. Silver — Superstore

# COMMAND ----------

df_super_raw = spark.table(f"{catalog}.bronze.superstore")
df_super_raw.printSchema()

# COMMAND ----------

df_super = (
    df_super_raw
    .transform(clean_column_names)
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id", "product_id"])
    .withColumn("order_date",  F.to_date(F.col("order_date"),  "M/d/yyyy"))
    .withColumn("ship_date",   F.to_date(F.col("ship_date"),   "M/d/yyyy"))
    .withColumn("sales",       F.col("sales").cast(T.DoubleType()))
    .withColumn("quantity",    F.col("quantity").cast(T.IntegerType()))
    .withColumn("discount",    F.col("discount").cast(T.DoubleType()))
    .withColumn("profit",      F.col("profit").cast(T.DoubleType()))
    .fillna({"sales": 0.0, "quantity": 0, "discount": 0.0, "profit": 0.0})
    # Clasificación de rentabilidad por fila
    .withColumn(
        "profit_category",
        F.when(F.col("profit") > 100, "Alto")
         .when(F.col("profit") > 0,   "Positivo")
         .when(F.col("profit") == 0,  "Neutro")
         .otherwise("Pérdida")
    )
    .withColumn("days_to_ship",
        F.datediff(F.col("ship_date"), F.col("order_date"))
    )
    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("_silver_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    df_super.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.superstore")
)

count = spark.table(f"{catalog}.silver.superstore").count()
print(f"[OK] {catalog}.silver.superstore -> {count:,} filas")

# COMMAND ----------

# MAGIC %md ## 3. Validación de calidad

# COMMAND ----------

from pyspark.sql.functions import count as cnt, isnan, when, col

def data_quality_report(table_name: str) -> None:
    df = spark.table(table_name)
    total = df.count()
    print(f"\n=== Reporte de Calidad: {table_name} ({total:,} filas) ===")

    numeric_types = (T.DoubleType, T.FloatType, T.IntegerType, T.LongType)
    null_counts = df.select([
        cnt(when(
            col(c).isNull() | (isnan(c) if isinstance(df.schema[c].dataType, numeric_types) else F.lit(False)),
            c
        )).alias(c)
        for c in df.columns if not isinstance(df.schema[c].dataType, T.StringType)
    ])
    null_counts.show(truncate=False)

data_quality_report(f"{catalog}.silver.ecommerce_sales")
data_quality_report(f"{catalog}.silver.superstore")
