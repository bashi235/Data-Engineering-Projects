import pandas as pd

# get the merge data
merge_data = pd.read_csv("../../final_data/merged_data.csv")

#create dim seller
dim_sellers = merge_data[["seller_id"]].drop_duplicates().copy()

# adding surrogate key
dim_sellers["seller_key"] = range(1,len(dim_sellers) + 1)

#reordering the attributes
dim_sellers = dim_sellers[["seller_key","seller_id"]]

#saving the dim seller
dim_sell = "../dim_csv/dim_sellers.csv"
dim_sellers.to_csv(dim_sell, index=False)
