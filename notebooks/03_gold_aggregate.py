# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Gold — Agregaciones y Tablas Analíticas
# MAGIC
# MAGIC Combina y agrega datos de Silver para producir tablas listas para consumo en
# MAGIC Databricks SQL Dashboards o Power BI.

# COMMAND ----------

dbutils.widgets.text("catalog", "etl_medallion", "Unity Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F

df_ecom  = spark.table(f"{catalog}.silver.ecommerce_sales")
df_super = spark.table(f"{catalog}.silver.superstore")

# COMMAND ----------

# MAGIC %md ## 1. Ventas por Categoría y Año (E-commerce)

# COMMAND ----------

gold_ecom_by_category = (
    df_ecom
    .groupBy("category", "sub_category", "order_year", "order_month")
    .agg(
        F.round(F.sum("sales"),       2).alias("total_sales"),
        F.round(F.sum("profit"),      2).alias("total_profit"),
        F.round(F.sum("revenue_net"), 2).alias("total_revenue_net"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("num_orders"),
        F.round(F.avg("profit_margin_pct"), 2).alias("avg_margin_pct"),
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
)

(
    gold_ecom_by_category.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.ecom_sales_by_category")
)
print(f"[OK] gold.ecom_sales_by_category -> {gold_ecom_by_category.count():,} filas")

# COMMAND ----------

# MAGIC %md ## 2. Rendimiento por Región y Segmento (Superstore)

# COMMAND ----------

gold_super_by_region = (
    df_super
    .groupBy("region", "segment", "category", "order_year")
    .agg(
        F.round(F.sum("sales"),  2).alias("total_sales"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("num_orders"),
        F.round(F.avg("days_to_ship"), 1).alias("avg_days_to_ship"),
        F.round(
            F.sum(F.when(F.col("profit_category") == "Pérdida", 1).otherwise(0))
            / F.count("*") * 100, 2
        ).alias("loss_rate_pct"),
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
)

(
    gold_super_by_region.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.super_performance_by_region")
)
print(f"[OK] gold.super_performance_by_region -> {gold_super_by_region.count():,} filas")

# COMMAND ----------

# MAGIC %md ## 3. Comparativa Consolidada por Categoría (join entre datasets)

# COMMAND ----------

ecom_agg = (
    df_ecom
    .groupBy("category", "order_year")
    .agg(
        F.round(F.sum("sales"), 2).alias("ecom_total_sales"),
        F.round(F.avg("profit_margin_pct"), 2).alias("ecom_avg_margin"),
    )
)

super_agg = (
    df_super
    .groupBy("category", "order_year")
    .agg(
        F.round(F.sum("sales"), 2).alias("super_total_sales"),
        F.round(F.avg(F.col("profit") / F.when(F.col("sales") > 0, F.col("sales")).otherwise(1)) * 100, 2)
         .alias("super_avg_margin"),
    )
)

gold_comparison = (
    ecom_agg
    .join(super_agg, on=["category", "order_year"], how="outer")
    .fillna(0.0)
    .withColumn(
        "sales_diff",
        F.round(F.col("ecom_total_sales") - F.col("super_total_sales"), 2)
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy("category", "order_year")
)

(
    gold_comparison.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.category_comparison")
)
print(f"[OK] gold.category_comparison -> {gold_comparison.count():,} filas")

# COMMAND ----------

# MAGIC %md ## 4. Top 10 Productos más Rentables (Superstore)

# COMMAND ----------

gold_top_products = (
    df_super
    .groupBy("product_id", "product_name", "category", "sub_category")
    .agg(
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.round(F.sum("sales"),  2).alias("total_sales"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_profit"))
    .limit(10)
    .withColumn("_gold_timestamp", F.current_timestamp())
)

(
    gold_top_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.top10_products")
)
print(f"[OK] gold.top10_products cargada")

# COMMAND ----------

# MAGIC %md ## 5. Resumen de Capas para Dashboard

# COMMAND ----------

gold_tables = [
    "ecom_sales_by_category",
    "super_performance_by_region",
    "category_comparison",
    "top10_products",
]

print("\n=== Resumen Capa Gold ===")
for t in gold_tables:
    n = spark.table(f"{catalog}.gold.{t}").count()
    print(f"  {catalog}.gold.{t:40s} -> {n:,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC Conecta desde **Databricks SQL → New Dashboard** usando las tablas `gold.*`
# MAGIC o apunta Power BI al endpoint JDBC/ODBC de este cluster.
