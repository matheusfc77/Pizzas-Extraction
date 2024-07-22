# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validando studies.silver.pizzas.FT_PRICE

# COMMAND ----------

limite_inferior = 0
limite_superior = 100
number_rows_with_invalid_price = (
    spark.table("studies.silver.pizzas")
        .filter(
            (F.col("FT_PRICE") <= limite_inferior) |
            (F.col("FT_PRICE") > limite_superior)
        ).count()
)

message = "Existem {} registros com preço fora dos limites válidos (> {} AND <= {})".format(
    number_rows_with_invalid_price, limite_inferior, limite_superior
)
assert number_rows_with_invalid_price == 0, message

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Validando studies.silver.pizza_details.NR_QUANTITY

# COMMAND ----------

limite_inferior = 0
limite_superior = 10
number_rows_with_invalid_quantity = (
    spark.table("studies.silver.order_details")
        .filter(
            (F.col("NR_QUANTITY") <= limite_inferior) |
            (F.col("NR_QUANTITY") > limite_superior)
        ).count()
)

message = "Existem {} registros com quantidade fora dos limites válidos (> {} AND <= {})".format(
    number_rows_with_invalid_quantity, limite_inferior, limite_superior
)
assert number_rows_with_invalid_quantity == 0, message

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validando studies.gold.dm_pizza

# COMMAND ----------

number_pizzas_with_more_than_one_active_version = spark.sql('''
    WITH pizzas_versions AS (
        SELECT
            CD_PIZZA,
            CD_DURABLE_PIZZA,
            RANK() OVER(PARTITION BY CD_DURABLE_PIZZA ORDER BY DT_EFFECTIVE_DATE)           AS RANK_VERSION
        FROM studies.gold.dm_pizza
       WHERE CURRENT_INDICATOR = 1
    )
    SELECT * FROM pizzas_versions
     WHERE RANK_VERSION > 1
''').count()

message = "Existem {} pizzas com mais de uma versão ativa.".format(
    number_pizzas_with_more_than_one_active_version, limite_inferior, limite_superior
)
assert number_pizzas_with_more_than_one_active_version == 0, message