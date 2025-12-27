import pandas as pd

# get the merge data
merge_data = pd.read_csv("../../final_data/merged_data.csv")

#create dim customers
dim_customers = merge_data[["customer_id", "customer_zip_code_prefix","customer_city","customer_state","customer_full_location"]].drop_duplicates().copy()

# adding surrogate key
dim_customers["customer_key"] = range(1,len(dim_customers) + 1)

#reordering the attributes
dim_customers = dim_customers[["customer_key","customer_id", "customer_zip_code_prefix","customer_city","customer_state","customer_full_location"]]

#saving the dim customers
dim_cust = "../dim_csv/dim_customers.csv"
dim_customers.to_csv(dim_cust, index=False)
