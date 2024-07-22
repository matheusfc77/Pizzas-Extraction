# Databricks notebook source
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultando dados da gold para ingestão

# COMMAND ----------

last_ingestion = spark.sql("select max(TT_INGESTION) from studies.gold.ft_orders").first()[0]
last_ingestion

last_ingestion_plus_delta = last_ingestion + datetime.timedelta(days=-1)
last_ingestion, last_ingestion_plus_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### ft_orders

# COMMAND ----------

df_orders = spark.sql(f'''
    SELECT
          dt.CD_ORDER,
          dt.CD_ORDER_DETAILS,
          pg.CD_PIZZA,
          CAST(YEAR(or.TT_CREATE_ORDER) || LPAD(MONTH(or.TT_CREATE_ORDER), 2, "0") || LPAD(DAY(or.TT_CREATE_ORDER), 2, "0") AS INT)   AS DK_CREATE_ORDER,
          dt.TT_INGESTION,
          dt.NR_QUANTITY,
          ps.FT_PRICE,
          dt.NR_QUANTITY * ps.FT_PRICE                                       AS TT_VALUE
      FROM studies.silver.order_details dt
      JOIN studies.silver.orders or
        ON dt.CD_ORDER = or.CD_ORDER
      JOIN studies.silver.pizzas ps
        ON dt.CD_PIZZA = ps.CD_PIZZA
      JOIN studies.gold.dm_pizza pg 
        ON dt.CD_PIZZA = pg.CD_SOURCE_PIZZA_CODE
       AND pg.CURRENT_INDICATOR = 1
     WHERE dt.TT_INGESTION > CAST("{last_ingestion_plus_delta.date()}" AS DATE)
''')

# COMMAND ----------

df_orders.createOrReplaceTempView("new_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO studies.gold.ft_orders
# MAGIC USING new_orders
# MAGIC   ON studies.gold.ft_orders.CD_ORDER = new_orders.CD_ORDER
# MAGIC   AND studies.gold.ft_orders.CD_ORDER_DETAILS = new_orders.CD_ORDER_DETAILS
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     CD_ORDER,
# MAGIC     CD_ORDER_DETAILS,  
# MAGIC     CD_PIZZA,
# MAGIC     DK_CREATE_ORDER,
# MAGIC     TT_INGESTION,
# MAGIC     NR_QUANTITY,
# MAGIC     FT_PRICE,
# MAGIC     TT_VALUE
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     new_orders.CD_ORDER,
# MAGIC     new_orders.CD_ORDER_DETAILS,
# MAGIC     new_orders.CD_PIZZA,
# MAGIC     new_orders.DK_CREATE_ORDER,
# MAGIC     new_orders.TT_INGESTION,
# MAGIC     new_orders.NR_QUANTITY,
# MAGIC     new_orders.FT_PRICE,
# MAGIC     new_orders.TT_VALUE
# MAGIC   )