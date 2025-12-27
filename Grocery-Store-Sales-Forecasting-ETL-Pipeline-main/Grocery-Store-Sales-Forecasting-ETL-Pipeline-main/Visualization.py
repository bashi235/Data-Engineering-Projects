# Databricks notebook source
# MAGIC %md
# MAGIC ### daily sales

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

df_daily = spark.table("grocery.grocery_etl_pipeline.daily_sales").toPandas()
daily_sales = df_daily.groupby("date")["daily_sales"].sum().reset_index()

plt.figure(figsize=(12, 5))
plt.scatter(daily_sales["date"], daily_sales["daily_sales"], marker='o')
plt.title("daily total sales trend")
plt.xlabel("date")
plt.ylabel("total sales")
plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ### weekly sales of storeNumber: 1

# COMMAND ----------

df_weekly = spark.table("grocery.grocery_etl_pipeline.weekly_sales").toPandas()

store_sample = df_weekly[df_weekly["store_nbr"] == 1]

plt.figure(figsize=(10,5))
plt.bar(store_sample["week"], store_sample["weekly_sales"])
plt.title("weekly sales for Store #1")
plt.xlabel("week")
plt.ylabel("sales")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### store performance

# COMMAND ----------

df_store = spark.table("grocery.grocery_etl_pipeline.store_performance").toPandas()
df_store_sorted = df_store.sort_values("total_sales", ascending=False)

plt.figure(figsize=(12,5))
plt.bar(df_store_sorted["store_nbr"].astype(str), df_store_sorted["total_sales"])
plt.title("total sales per store")
plt.xlabel("store number")
plt.ylabel("total sales")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### family performance

# COMMAND ----------

df_family = spark.table("grocery.grocery_etl_pipeline.family_performance").toPandas()
df_family_sorted = df_family.sort_values("total_sales", ascending=False).head(5)

plt.figure(figsize=(12,5))
plt.bar(df_family_sorted["family"], df_family_sorted["total_sales"])
plt.title("sales by product category")
plt.xlabel("Product category")
plt.ylabel("total sales")
plt.show()

# COMMAND ----------

