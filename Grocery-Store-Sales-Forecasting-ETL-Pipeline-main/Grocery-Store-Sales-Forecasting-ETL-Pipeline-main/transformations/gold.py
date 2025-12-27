import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
#------------------------------------------------------
# Load Silver as Views
@dlt.view
def merged_v():
    return spark.table("LIVE.merged_sales")

@dlt.table
def daily_sales():
    df = spark.table("LIVE.merged_v")

    return (
        df.groupBy("date", "store_nbr", "family")
          .agg(
              sum("sales").alias("daily_sales"),
              sum("onpromotion").alias("daily_promotions"),
              avg("transactions").alias("daily_traffic"),
              avg("dcoilwtico").alias("oil_price"),
              max("is_national_holiday").alias("holiday_flag")
          )
    )
#----------------------------------------
#  WEEKLY SALES
@dlt.table
def weekly_sales():
    df = spark.table("LIVE.merged_v").withColumn("week", weekofyear("date"))
    return (
        df.groupBy("store_nbr", "family", "week")
          .agg(
              sum("sales").alias("weekly_sales"),
              sum("onpromotion").alias("weekly_promotions"),
              avg("transactions").alias("avg_traffic"),
              avg("dcoilwtico").alias("avg_oil_price")
          )
    )
#-----------------------------------------------------------
# STORE PERFORMANCE
@dlt.table
def store_performance():
    df = spark.table("LIVE.merged_v")
    return (
        df.groupBy("store_nbr", "city", "state", "type", "cluster")
          .agg(
              sum("sales").alias("total_sales"),
              avg("sales").alias("avg_daily_sales"),
              avg("transactions").alias("avg_daily_traffic"),
              avg("dcoilwtico").alias("avg_oil_price")
          )
    )
#-----------------------------------------------------
# FAMILY (CATAGORY) PERFORMANCE
@dlt.table
def family_performance():
    df = spark.table("LIVE.merged_v")
    return (
        df.groupBy("family")
          .agg(
              sum("sales").alias("total_sales"),
              avg("sales").alias("avg_daily_sales"),
              sum("onpromotion").alias("total_promotions")
          )
    )
#---------------------------------------------------

