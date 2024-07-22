# Databricks notebook source
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ### Consultando a última data de ingestão

# COMMAND ----------

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.silver.pizza_types").first()[0]

# COMMAND ----------

# MAGIC %md ### Lendo dados da camada bronze

# COMMAND ----------

df_bronze = spark.sql(f"select * from studies.bronze.pizza_types where TT_INGESTION > '{last_ingestion}'")

# COMMAND ----------

# MAGIC %md ### Tratamento e limpeza dos dados

# COMMAND ----------

df_bronze = (
    df_bronze
        .withColumn("NM_PIZZA_NAME",                        F.trim(F.upper(F.col("NM_PIZZA_NAME"))))
        .withColumn("CT_PIZZA_CATEGORY",                    F.trim(F.upper(F.col("CT_PIZZA_CATEGORY"))))
        .withColumn("DS_INGREDIENTS",                       F.trim(F.upper(F.col("DS_INGREDIENTS"))))
)

# COMMAND ----------

# MAGIC %md ### Persistindo na camada silver

# COMMAND ----------

(
    df_bronze
        .write.format("delta")
        .mode("append")
        .saveAsTable("studies.silver.pizza_types")
)