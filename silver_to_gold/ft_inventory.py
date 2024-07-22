# Databricks notebook source
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo dados da camada silver

# COMMAND ----------

last_ingestion_plus_delta = (datetime.datetime.now() + datetime.timedelta(days=-1)).date()
last_ingestion_plus_delta_formatted = (
    int(str(last_ingestion_plus_delta.year) + 
    str(last_ingestion_plus_delta.month).rjust(2, '0') + 
    str(last_ingestion_plus_delta.day).rjust(2, '0'))
)
last_ingestion_plus_delta_formatted

# COMMAND ----------

df_inventory = spark.sql(f'''
    SELECT 
        or.DK_CREATE_ORDER                                   AS DK_INVENTORY_SNAPSHOT,
        bd.CD_INGREDIENT,
        COUNT(1)                                             AS NR_QUANTITY
      FROM studies.gold.ft_orders or
      JOIN studies.gold.dm_pizza pg
        ON or.CD_PIZZA = pg.CD_PIZZA
      JOIN studies.gold.bd_bridge_ingredients_pizzas bd
        ON bd.CD_DURABLE_PIZZA = pg.CD_DURABLE_PIZZA
     WHERE or.DK_CREATE_ORDER = {last_ingestion_plus_delta_formatted}
     GROUP BY or.DK_CREATE_ORDER, bd.CD_INGREDIENT
''')

# COMMAND ----------

df_inventory.createOrReplaceTempView("new_inventory")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO studies.gold.ft_inventory
# MAGIC USING new_inventory
# MAGIC   ON studies.gold.ft_inventory.DK_INVENTORY_SNAPSHOT = new_inventory.DK_INVENTORY_SNAPSHOT
# MAGIC   AND studies.gold.ft_inventory.CD_INGREDIENT = new_inventory.CD_INGREDIENT
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     DK_INVENTORY_SNAPSHOT,
# MAGIC     CD_INGREDIENT,
# MAGIC     NR_QUANTITY
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     new_inventory.DK_INVENTORY_SNAPSHOT, 
# MAGIC     new_inventory.CD_INGREDIENT ,
# MAGIC     new_inventory.NR_QUANTITY 
# MAGIC   )