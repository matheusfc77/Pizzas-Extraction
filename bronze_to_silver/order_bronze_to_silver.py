# Databricks notebook source
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

import datetime

# COMMAND ----------

# MAGIC %md ### Consultando a última data de ingestão

# COMMAND ----------

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.silver.orders").first()[0]

# COMMAND ----------

# MAGIC %md ### Lendo dados da camada bronze

# COMMAND ----------

df_bronze = spark.sql(f"select * from studies.bronze.orders where TT_INGESTION > '{last_ingestion}'")

# COMMAND ----------

# MAGIC %md ### Tratamento e limpeza dos dados

# COMMAND ----------

df_bronze = (
    df_bronze
        .withColumn("CD_ORDER",                             F.col("CD_ORDER").cast(IntegerType()))
        .withColumn("TT_CREATE_ORDER",                      F.to_timestamp(F.concat(F.col("DT_CREATION_DATE"), F.lit(" "),  F.date_format('TM_CREATION_TIME', 'HH:mm:ss'))))
        .drop("DT_CREATION_DATE", "TM_CREATION_TIME")
        .select("CD_ORDER", "TT_CREATE_ORDER", "TT_INGESTION", "NR_YEAR", "NR_MONTH")
)

# COMMAND ----------

# MAGIC %md ### Persistindo na camada silver

# COMMAND ----------

(
    df_bronze
        .write.format("delta")
        .mode("append")
        .partitionBy("NR_YEAR", "NR_MONTH")
        .saveAsTable("studies.silver.orders")
)