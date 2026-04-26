# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Bronze — Ingesta de Datos Crudos
# MAGIC
# MAGIC Conecta a Azure Data Lake Storage Gen2 usando **Managed Identity** y escribe los datasets
# MAGIC en tablas Delta sin transformaciones. Se agregan columnas de auditoría.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("catalog", "etl_medallion", "Unity Catalog Name")
dbutils.widgets.text("container_raw", "raw", "Container Raw")

storage_account = dbutils.widgets.get("storage_account")
catalog         = dbutils.widgets.get("catalog")
container_raw   = dbutils.widgets.get("container_raw")

# COMMAND ----------

# MAGIC %md ## 1. Rutas de acceso via Unity Catalog External Location

# COMMAND ----------

print(f"Leyendo desde External Location: abfss://raw@{storage_account}.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md ## 2. Crear base de datos Bronze si no existe

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

# MAGIC %md ## 3. Rutas de origen en ADLS Gen2

# COMMAND ----------

from pyspark.sql import functions as F

base_path = f"abfss://{container_raw}@{storage_account}.dfs.core.windows.net"

paths = {
    "ecommerce_sales": f"{base_path}/ecommerce_sales/",
    "superstore":      f"{base_path}/superstore/",
}

# COMMAND ----------

# MAGIC %md ## 4. Función de ingesta genérica

# COMMAND ----------

def ingest_csv_to_bronze(source_path: str, table_name: str) -> None:
    """Lee CSV desde ADLS y persiste como tabla Delta en la capa Bronze."""
    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(source_path)
    )

    renamed = {c: c.strip().lower().replace(" ", "_").replace("-", "_") for c in raw.columns}
    for old, new in renamed.items():
        if old != new:
            raw = raw.withColumnRenamed(old, new)

    df = (
        raw
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    full_table = f"{catalog}.bronze.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )

    count = spark.table(full_table).count()
    print(f"[OK] {full_table} -> {count:,} filas cargadas")

# COMMAND ----------

# MAGIC %md ## 5. Ingesta de datasets

# COMMAND ----------

ingest_csv_to_bronze(paths["ecommerce_sales"], "ecommerce_sales")
ingest_csv_to_bronze(paths["superstore"],      "superstore")

# COMMAND ----------

# MAGIC %md ## 6. Validación rápida

# COMMAND ----------

for table in ["ecommerce_sales", "superstore"]:
    print(f"\n--- {catalog}.bronze.{table} ---")
    spark.table(f"{catalog}.bronze.{table}").printSchema()
    spark.table(f"{catalog}.bronze.{table}").show(3, truncate=False)
