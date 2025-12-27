import pandas as pd

# get the merge data
merge_data = pd.read_csv("../../final_data/merged_data.csv")

#create dim payments
dim_payments = merge_data[["payment_type"]].drop_duplicates().copy()

# adding surrogate key
dim_payments["payment_key"] = range(1,len(dim_payments) + 1)

#reordering the attributes
dim_payments = dim_payments[["payment_key","payment_type"]]

#saving the dim payments
dim_pay = "../dim_csv/dim_payments.csv"
dim_payments.to_csv(dim_pay, index=False)


