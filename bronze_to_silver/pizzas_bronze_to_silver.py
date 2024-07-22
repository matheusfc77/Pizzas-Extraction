# Databricks notebook source
from pyspark.sql.types import IntegerType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ### Consultando a última data de ingestão

# COMMAND ----------

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.silver.pizzas").first()[0]

# COMMAND ----------

# MAGIC %md ### Lendo dados da camada bronze

# COMMAND ----------

df_bronze = spark.sql(f"select * from studies.bronze.pizzas where TT_INGESTION > '{last_ingestion}'")

# COMMAND ----------

# MAGIC %md ### Tratamento e limpeza dos dados

# COMMAND ----------

df_bronze = (
    df_bronze
        .withColumn("CT_SIZE",                              F.trim(F.upper(F.col("CT_SIZE"))))
        .withColumn("FT_PRICE",                             F.col("FT_PRICE").cast(FloatType()))
)

# COMMAND ----------

# MAGIC %md ### Persistindo na camada silver

# COMMAND ----------

(
    df_bronze
        .write.format("delta")
        .mode("append")
        .saveAsTable("studies.silver.pizzas")
)