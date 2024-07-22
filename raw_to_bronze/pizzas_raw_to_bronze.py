# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType
import pyspark.sql.functions as F

import os
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo dados da camada raw/landing

# COMMAND ----------

df_raw = spark.sql("select * from hive_metastore.default.pizzas")

# COMMAND ----------

# MAGIC %md ### Renomeando colunas e criado colunas para particionamento

# COMMAND ----------

df_raw = (
    df_raw
        .withColumnRenamed("pizza_id",                      "CD_PIZZA")
        .withColumnRenamed("pizza_type_id",                 "CD_PIZZA_TYPE")
        .withColumnRenamed("size",                          "CT_SIZE")
        .withColumnRenamed("price",                         "FT_PRICE")
        .withColumn("TT_INGESTION",                         F.lit(datetime.datetime.now()))
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
#         .saveAsTable("studies.bronze.pizzas")
# )