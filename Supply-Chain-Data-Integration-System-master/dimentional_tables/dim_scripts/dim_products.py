import pandas as pd

# get the merge data
merge_data = pd.read_csv("../../final_data/merged_data.csv")

#create dim products
dim_products = merge_data[["product_id", "product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm","product_volume_cm3","product_weight_category"]].drop_duplicates().copy()

# adding surrogate key
dim_products["product_key"] = range(1,len(dim_products) + 1)

#reordering the attributes
dim_products = dim_products[["product_key","product_id", "product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm","product_volume_cm3","product_weight_category"]]

#saving the dim products
dim_prod = "../dim_csv/dim_products.csv"
dim_products.to_csv(dim_prod, index=False)
