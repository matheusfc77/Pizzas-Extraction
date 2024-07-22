# Databricks notebook source
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ### Consultando a última data de ingestão

# COMMAND ----------

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.silver.order_details").first()[0]

# COMMAND ----------

# MAGIC %md ### Lendo dados da camada bronze

# COMMAND ----------

df_bronze = spark.sql(f"select * from studies.bronze.order_details where TT_INGESTION > '{last_ingestion}'")

# COMMAND ----------

# MAGIC %md ### Tratamento e limpeza dos dados

# COMMAND ----------

df_bronze = (
    df_bronze
        .withColumn("CD_ORDER_DETAILS",                     F.col("CD_ORDER_DETAILS").cast(IntegerType()))
        .withColumn("CD_ORDER",                             F.col("CD_ORDER").cast(IntegerType()))
        .withColumn("NR_QUANTITY",                          F.col("NR_QUANTITY").cast(IntegerType()))
)

# COMMAND ----------

# MAGIC %md ### Persistindo na camada silver

# COMMAND ----------

(
    df_bronze
        .write.format("delta")
        .mode("append")
        .partitionBy("NR_YEAR", "NR_MONTH")
        .saveAsTable("studies.silver.order_details")
)