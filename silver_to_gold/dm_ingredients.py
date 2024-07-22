# Databricks notebook source
# MAGIC %md
# MAGIC ### Consultando dados da gold para ingestão

# COMMAND ----------

max_row_number = spark.sql("select max(CD_INGREDIENT) from studies.gold.dm_ingredient").first()[0] 
print(max_row_number)

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.gold.dm_ingredient").first()[0]
last_ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### dm_ingredients

# COMMAND ----------

df_map_pizzas_ingredients = spark.sql(f'''
    WITH ingredients_list AS (
        SELECT 
            CD_PIZZA_TYPE, 
            NM_PIZZA_NAME,
            EXPLODE(SPLIT(DS_INGREDIENTS, ",")) AS NM_INGREDIENT,
            TT_INGESTION
        FROM studies.silver.pizza_types
       WHERE TT_INGESTION > '{last_ingestion}'
    )
    SELECT CD_PIZZA_TYPE, NM_PIZZA_NAME, TRIM(NM_INGREDIENT) AS NM_INGREDIENT, TT_INGESTION FROM ingredients_list
''')

# COMMAND ----------

df_map_pizzas_ingredients.createOrReplaceTempView("map_pizzas_ingredients")

# COMMAND ----------

df_ingredients = spark.sql(f'''
  WITH ingredients AS (
      SELECT * FROM (
        SELECT 
            NM_INGREDIENT,
            TT_INGESTION,
            RANK() OVER(PARTITION BY NM_INGREDIENT ORDER BY TT_INGESTION DESC, CD_PIZZA_TYPE)           AS RANK_VERSION 
          FROM map_pizzas_ingredients
      ) aux
      WHERE aux.RANK_VERSION = 1
  )
  SELECT 
      ROW_NUMBER() OVER(ORDER BY silver.NM_INGREDIENT) + {max_row_number}                               AS CD_INGREDIENT,
      silver.NM_INGREDIENT,
      silver.TT_INGESTION
    FROM ingredients silver
   WHERE NOT EXISTS (
      SELECT 1 FROM studies.gold.dm_ingredient gold WHERE silver.NM_INGREDIENT = gold.NM_INGREDIENT
    )
  UNION ALL
  SELECT 
      gold.CD_INGREDIENT,
      silver.NM_INGREDIENT,
      silver.TT_INGESTION
    FROM ingredients silver
    JOIN studies.gold.dm_ingredient gold
      ON silver.NM_INGREDIENT = gold.NM_INGREDIENT
''')

# COMMAND ----------

df_ingredients.createOrReplaceTempView("new_ingredients")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO studies.gold.dm_ingredient
# MAGIC USING new_ingredients
# MAGIC   ON studies.gold.dm_ingredient.CD_INGREDIENT = new_ingredients.CD_INGREDIENT
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     TT_INGESTION = new_ingredients.TT_INGESTION
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     CD_INGREDIENT,
# MAGIC     NM_INGREDIENT,
# MAGIC     TT_INGESTION
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     new_ingredients.CD_INGREDIENT,
# MAGIC     new_ingredients.NM_INGREDIENT,
# MAGIC     new_ingredients.TT_INGESTION
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bridge

# COMMAND ----------

df_bd_ingredients_pizzas = spark.sql('''
    SELECT
          ing.CD_INGREDIENT,
          pg.CD_DURABLE_PIZZA
      FROM map_pizzas_ingredients map
      JOIN studies.gold.dm_ingredient ing
        ON map.NM_INGREDIENT = ing.NM_INGREDIENT
      JOIN studies.gold.dm_pizza pg
        ON pg.NM_PIZZA_NAME = map.NM_PIZZA_NAME
     WHERE pg.CURRENT_INDICATOR = 1
''')

# COMMAND ----------

df_bd_ingredients_pizzas.createOrReplaceTempView("bd_ingredients_pizzas")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO studies.gold.bd_bridge_ingredients_pizzas
# MAGIC USING bd_ingredients_pizzas
# MAGIC   ON studies.gold.bd_bridge_ingredients_pizzas.CD_INGREDIENT = bd_ingredients_pizzas.CD_INGREDIENT
# MAGIC   AND studies.gold.bd_bridge_ingredients_pizzas.CD_DURABLE_PIZZA = bd_ingredients_pizzas.CD_DURABLE_PIZZA
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     CD_INGREDIENT,
# MAGIC     CD_DURABLE_PIZZA
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     bd_ingredients_pizzas.CD_INGREDIENT,
# MAGIC     bd_ingredients_pizzas.CD_DURABLE_PIZZA
# MAGIC   )