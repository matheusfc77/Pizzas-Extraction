# Databricks notebook source
import pyspark.sql.functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %md ### Gerando dm_date

# COMMAND ----------

def create_date_table(start='2000-01-01', end='2050-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["NR_YEAR"] = df.Date.dt.year
    df["NR_MONTH"] = df.Date.dt.month
    df["NR_DAY"] = df.Date.dt.day
    df["NR_WEEK"] = df.Date.dt.weekofyear
    df["NR_QUARTER"] = df.Date.dt.quarter
    df["NM_DAY"] = df.Date.dt.day_name()
    df["NR_DAY_OF_WEEK"] = df.Date.dt.day_of_week
    df["NM_MONTH"] = df.Date.dt.month_name()
    df["CD_DATE"] = df["Date"].map(lambda info: int(str(info.year) + str(info.month).rjust(2, '0') + str(info.day).rjust(2, '0')))
    df.rename(columns={"Date":"DT_DATE"}, inplace=True)
    return df

# COMMAND ----------

# dados de vendas são do ano de 2015
df_date_pandas = create_date_table(start='2015-01-01', end='2020-12-31')
df_date = spark.createDataFrame(df_date_pandas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persistindo na camada gold

# COMMAND ----------

(
    df_date
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("studies.gold.dm_date")
)