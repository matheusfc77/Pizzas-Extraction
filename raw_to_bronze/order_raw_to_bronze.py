# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType
import pyspark.sql.functions as F

import os
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo dados da camada raw/landing

# COMMAND ----------

df_raw = spark.sql("select * from hive_metastore.default.orders")

# COMMAND ----------

# MAGIC %md ### Renomeando colunas e criado colunas para particionamento

# COMMAND ----------

df_raw = (
    df_raw
        .withColumnRenamed("order_id",                      "CD_ORDER")
        .withColumnRenamed("date",                          "DT_CREATION_DATE")
        .withColumnRenamed("time",                          "TM_CREATION_TIME")
        .withColumn("TT_INGESTION",                         F.lit(datetime.datetime.now()))
        .withColumn("NR_YEAR",                              F.year(F.col("TT_INGESTION")))
        .withColumn("NR_MONTH",                             F.month(F.col("TT_INGESTION")))
)

# COMMAND ----------

# MAGIC %md ### Persitindo na camada bronze

# COMMAND ----------

# como os dados vindos das fontes não possuem datetime, não implementei o filtro incremental da row_to_bronze
# seria necessário fazer esse ajuste antes de subir para prod

# (
#     df_raw
#         .write.format("delta")
#         .mode("append")
#         .partitionBy("NR_YEAR", "NR_MONTH")
#         .saveAsTable("studies.bronze.orders")
# )