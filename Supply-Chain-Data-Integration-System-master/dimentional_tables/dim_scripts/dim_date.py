import pandas as pd

# get the merge data
merge_data = pd.read_csv("../../final_data/merged_data.csv")

# extracting all dates into a single attribute
date_series = pd.concat([pd.to_datetime(merge_data["order_purchase_timestamp"]),pd.to_datetime(merge_data["order_approved_at"]),pd.to_datetime(merge_data["ship_date"])]).drop_duplicates().sort_values().reset_index(drop=True)

# create dim dates
dim_dates = pd.DataFrame()
dim_dates["date"] = date_series

# adding date attributes
dim_dates["year"] = dim_dates["date"].dt.year
dim_dates["month"] = dim_dates["date"].dt.month
dim_dates["day"] = dim_dates["date"].dt.day
dim_dates["week"] = dim_dates["date"].dt.isocalendar().week
dim_dates["quarter"] = dim_dates["date"].dt.quarter
dim_dates["day_of_week"] = dim_dates["date"].dt.day_name()

# adding surrogate key
dim_dates["date_key"] = range(1, len(dim_dates) + 1)

#reordering the attributes
dim_dates = dim_dates[["date_key", "date", "year", "month", "day", "week", "quarter", "day_of_week"]]

#saving the dim payments
dim_date = "../dim_csv/dim_dates.csv"
dim_dates.to_csv(dim_date, index=False)