import pandas as pd

orders_data = pd.read_csv('../data_resources/Orders.csv')

# print(orders_data.isnull().sum())
# order_id                    0
# customer_id                 0
# order_purchase_timestamp    0
# order_approved_at           7

# converting    order_purchase_timestamp, order_approved_at to datetime
orders_data['order_purchase_timestamp'] = pd.to_datetime(orders_data['order_purchase_timestamp'])
orders_data['order_approved_at'] = pd.to_datetime(orders_data['order_approved_at'])

#----------------------------------------------------------------------------------
# having nan values in the order_approved_at attribute , so manage that nan values to same as order_purchase_timestamp
# so that saying, order is approved when it was purchased ;

orders_data['order_approved_at'] = orders_data['order_approved_at'].fillna(orders_data['order_purchase_timestamp'])

#-----------------------------------------------------------------------------------

# to check the performance of the order placing we can check that using the diff(order_approved_at - order_purchase_timestamp)
orders_data['order_approved_time_in_seconds'] = (orders_data['order_approved_at'] - orders_data['order_purchase_timestamp']).dt.total_seconds()


#----------------------------------------------------------------------------------

# the updated record is saving in a new file
new_orders_data = "../transform_data/updated_data/Orders.csv"
orders_data.to_csv(new_orders_data,index=False)
