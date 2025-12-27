import pandas as pd
import random

# Load all updated datasets
customers = pd.read_csv("../transform_data/updated_data/Customers.csv")
order_items = pd.read_csv("../transform_data/updated_data/OrderItems.csv")
orders = pd.read_csv("../transform_data/updated_data/Orders.csv")
payments = pd.read_csv("../transform_data/updated_data/Payments.csv")
products = pd.read_csv("../transform_data/updated_data/Products.csv")

# Merging all csv files
#orders->customers
#orders -> order_items
#orders -> payments
# orders -> products
merged = (
    orders
    .merge(customers, on="customer_id", how="left")
    .merge(order_items, on="order_id", how="left")
    .merge(payments, on="order_id", how="left")
    .merge(products, on="product_id", how="left")
)

# add attributes

# to find time taken from when an order is placed and until it is approved - LEAD TIME
merged['lead_time_days'] = (
    pd.to_datetime(merged['order_approved_at']) -
    pd.to_datetime(merged['order_purchase_timestamp'])
).dt.days
# need to find the ship date since we do not have ship date in dataset
# so i need generate a random no. between 3 to 7 estimating that time taken atleast 3 days and maximum 7
# axis=1 --> process row by row
# pd.to_timedelta(value, unit='D') this will add days like 2 days, 3 days etc....
merged['ship_date'] = (
    pd.to_datetime(merged['order_approved_at']) +
    pd.to_timedelta(merged.apply(lambda x: random.randint(3, 7), axis=1), unit='D')
)

# order cycle time = >the total time from when the customer places the order to until the order is shipped.
merged['order_cycle_days'] = (merged['ship_date'] - pd.to_datetime(merged['order_purchase_timestamp'])).dt.days

# cost price -assume that seller get 10% margin
merged['seller_cost'] = merged['price'] * 0.90

# profit that the seller gets
merged['seller_profit'] = merged['total_charges'] - merged['seller_cost']

# seller profits margin
merged['seller_profit_margin'] = (merged['seller_profit'] / merged['total_charges']) * 100

# Shipping Efficiency tells if it have --
# Higher then better, efficient shipping
# Lower then expensive and poor shipping value
merged['shipping_efficiency'] = merged.apply(lambda row: row['total_charges'] / row['shipping_charges'] if row['shipping_charges'] != 0 else None,axis=1)

# concat the full customer location
merged['customer_full_location'] = merged['customer_city'] + ", " + merged['customer_state'] + " " +merged['customer_zip_code_prefix'].astype(str)


# saving the merged file
new_merged_data = "../final_data/merged_data.csv"
merged.to_csv(new_merged_data, index=False)