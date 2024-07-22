# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
import pyspark.sql.functions as F

import datetime

# COMMAND ----------

max_row_number = spark.sql("select max(CD_PIZZA) from studies.gold.dm_pizza").first()[0] 
print(max_row_number)

last_ingestion = spark.sql("select max(DT_EFFECTIVE_DATE) from studies.gold.dm_pizza").first()[0]
last_ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicando SCD tipo 2 em dm_pizza

# COMMAND ----------

# seleciona novas pizzas ou pizzas que tiveram o campo "CT_PIZZA_CATEGORY" alterado 
df_pizza_scd = spark.sql(f'''
    WITH pizza_versions AS (
        SELECT *
          FROM (
            SELECT 
                CD_SOURCE_PIZZA_CODE,
                CD_PIZZA,
                RANK() OVER(PARTITION BY CD_SOURCE_PIZZA_CODE ORDER BY DT_EFFECTIVE_DATE)           AS RANK_PIZZA_VERSIONS
            FROM studies.gold.dm_pizza
        ) aux
          WHERE aux.RANK_PIZZA_VERSIONS = 1
    )
    SELECT
        ROW_NUMBER() OVER(ORDER BY pz.CD_PIZZA_TYPE) + {max_row_number}                             AS CD_PIZZA,
        COALESCE(pv.CD_PIZZA, ROW_NUMBER() OVER(ORDER BY pz.CD_PIZZA_TYPE) + {max_row_number})      AS CD_DURABLE_PIZZA, 
        pz.CD_PIZZA                                                                                 AS CD_SOURCE_PIZZA_CODE,
        pz.CT_SIZE,
        pt.NM_PIZZA_NAME,
        pt.CT_PIZZA_CATEGORY,
        pt.TT_INGESTION                                                                             AS DT_EFFECTIVE_DATE,
        TIMESTAMP('9999-1-1')                                                                       AS EXPIRATION_DATE,
        CAST(1 AS SMALLINT)                                                                         AS CURRENT_INDICATOR
      FROM studies.silver.pizzas pz
      LEFT JOIN studies.silver.pizza_types pt
        ON pz.CD_PIZZA_TYPE = pt.CD_PIZZA_TYPE
      LEFT JOIN pizza_versions pv 
        ON pz.CD_PIZZA = pv.CD_SOURCE_PIZZA_CODE
     WHERE pz.TT_INGESTION > '{last_ingestion}'
        OR pt.TT_INGESTION > '{last_ingestion}'
''') 

# COMMAND ----------

df_pizza_scd.createOrReplaceTempView('pizza_scd')

# COMMAND ----------

(
    df_pizza_scd
        .write.format("delta")
        .mode("append")
        .saveAsTable("studies.gold.dm_pizza")
)

# COMMAND ----------

# seleciona as versões que expiraram e prepara os campos "EXPIRATION_DATE" e "CURRENT_INDICATOR" para atualização
df_update_old_version = spark.sql(f'''
    WITH new_pizzas AS (
        SELECT
            dp.*,
            RANK() OVER(
                PARTITION BY dp.CD_SOURCE_PIZZA_CODE 
                ORDER BY dp.DT_EFFECTIVE_DATE DESC
            )                                                 AS RANK_VERSION
        FROM studies.gold.dm_pizza dp
        JOIN pizza_scd
            ON dp.CD_SOURCE_PIZZA_CODE = pizza_scd.CD_SOURCE_PIZZA_CODE
    ),
    new_experation_date AS (
        SELECT
            pt1.CD_PIZZA_TYPE,
            pt1.TT_INGESTION,
            RANK() OVER(PARTITION BY pt1.CD_PIZZA_TYPE ORDER BY pt1.TT_INGESTION DESC)           AS RANK_EXPIRATION_DATE
          FROM studies.silver.pizza_types pt1
    )                              
    SELECT 
        np.CD_PIZZA,
        np.CD_SOURCE_PIZZA_CODE,
        np.CT_SIZE,
        np.NM_PIZZA_NAME,
        np.CT_PIZZA_CATEGORY,
        np.DT_EFFECTIVE_DATE,
        ed.TT_INGESTION                                       AS EXPIRATION_DATE,
        CAST(0 AS SMALLINT)                                   AS CURRENT_INDICATOR
      FROM new_pizzas np
      JOIN studies.silver.pizzas pz
        ON np.CD_SOURCE_PIZZA_CODE = pz.CD_PIZZA
      JOIN new_experation_date ed
        ON pz.CD_PIZZA_TYPE = ed.CD_PIZZA_TYPE
     WHERE np.RANK_VERSION > 1 
       AND np.CURRENT_INDICATOR = 1
       AND ed.RANK_EXPIRATION_DATE = 1
''')

# COMMAND ----------

df_update_old_version.createOrReplaceTempView('update_old_version')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO studies.gold.dm_pizza
# MAGIC USING update_old_version
# MAGIC   ON studies.gold.dm_pizza.CD_PIZZA = update_old_version.CD_PIZZA
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     EXPIRATION_DATE = update_old_version.EXPIRATION_DATE,
# MAGIC     CURRENT_INDICATOR = update_old_version.CURRENT_INDICATOR