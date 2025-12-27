import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

S3_BASE = "s3://store-sales-forecasting-bashi-bucket/raw/"
#--------------------------------------------------------------------
# SALES(train.csv)
@dlt.table
def sales_bronze():
    return (
        spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{S3_BASE}sales/train.csv")
    )
#-------------------------------------------------------
#TRANSACTIONS
@dlt.table
def transactions_bronze():
    return (
        spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{S3_BASE}transactions/transactions.csv")
            .withColumn("date", to_date(col("date")))
    )
#-----------------------------------------------------------
#STORES
@dlt.table
def stores_bronze():
    return (
        spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{S3_BASE}stores/stores.csv")
    )
#---------------------------------------------------
#HOLIDAYS
@dlt.table
def holidays_bronze():
    return (
        spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{S3_BASE}holidays/holidays_events.csv")
            .withColumn("date", to_date(col("date")))
    )
#--------------------------------------------------
# OIL
@dlt.table
def oil_bronze():
    return (
        spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{S3_BASE}oil/oil.csv")
            .withColumn("date", to_date(col("date")))
            .withColumn("dcoilwtico", col("dcoilwtico").cast("double"))
    )
