import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
#--------------------------------------------------------
# Load Bronze as Views
@dlt.view
def sales_bronze_v():
    return spark.table("LIVE.sales_bronze")

@dlt.view
def stores_bronze_v():
    return spark.table("LIVE.stores_bronze")

@dlt.view
def transactions_bronze_v():
    return spark.table("LIVE.transactions_bronze")

@dlt.view
def holidays_bronze_v():
    return spark.table("LIVE.holidays_bronze")

@dlt.view
def oil_bronze_v():
    return spark.table("LIVE.oil_bronze")

#-------------------------------------------------------------
# CLEAN SALES
@dlt.table
@dlt.expect("valid_sales", "sales >= 0")
def sales_clean():
    df = spark.table("LIVE.sales_bronze_v")
    return (
        df.withColumn("date", col("date").cast("date"))
          .withColumn("sales", when(col("sales") < 0, 0).otherwise(col("sales")))
          .dropDuplicates(["date", "store_nbr", "family"])
    )
#------------------------------------------------------------------------------
# CLEAN STORES
@dlt.table
def stores_clean():
    return spark.table("LIVE.stores_bronze_v").dropDuplicates(["store_nbr"])
#-----------------------------------------------------------------------
# CLEAN HOLIDAYS
@dlt.table
def holidays_clean():
    df = spark.table("LIVE.holidays_bronze_v")
    return (
        df.filter(col("transferred") == False)
          .select(
              col("date"),
              when(col("type") == "National", 1).otherwise(0).alias("is_national_holiday")
          )
          .dropDuplicates(["date"])
    )

#--------------------------------------------------------------
# CLEAN TRANSACTIONS
@dlt.table
def transactions_clean():
    return (
        spark.table("LIVE.transactions_bronze_v")
            .dropDuplicates(["date", "store_nbr"])
    )
#--------------------------------------------------------------------
# CLEAN & IMPUTE OIL PRICES
@dlt.table
def oil_clean():
    df = spark.table("LIVE.oil_bronze_v").orderBy("date")
    w = Window.orderBy("date")
    df2 = df.withColumn(
        "dcoilwtico",
        when(col("dcoilwtico").isNotNull(), col("dcoilwtico"))
        .otherwise(lag("dcoilwtico").over(w))
    )
    return df2.fillna({"dcoilwtico": 0})
#--------------------------------------------------------
# SILVER MERGED TABLE
@dlt.table
@dlt.expect_or_drop("valid_store", "store_nbr IS NOT NULL")
def merged_sales():
    sales = spark.table("LIVE.sales_clean")
    stores = spark.table("LIVE.stores_clean")
    tx = spark.table("LIVE.transactions_clean")
    hol = spark.table("LIVE.holidays_clean")
    oil = spark.table("LIVE.oil_clean")
    df = (
        sales.join(stores, "store_nbr", "left")
             .join(tx, ["date", "store_nbr"], "left")
             .join(oil, "date", "left")
             .join(hol, "date", "left")
             .fillna({"is_national_holiday": 0})
    )
    return df

#-------------------------------------------------------------

#ADD REJECTED RECORDS IN ERROR_LOGS

@dlt.table
def error_logs():
    df = spark.table("LIVE.sales_clean") \
             .join(spark.table("LIVE.stores_clean"), "store_nbr", "left") \
             .join(spark.table("LIVE.transactions_clean"), ["date", "store_nbr"], "left") \
             .join(spark.table("LIVE.oil_clean"), "date", "left") \
             .join(spark.table("LIVE.holidays_clean"), "date", "left")

    return df.filter("store_nbr IS NULL")
