import pandas as pd

order_items_data = pd.read_csv("../data_resources/OrderItems.csv")

#print(order_items_data.isnull().sum())
# we got no nan values

#converting price , shipping_charges to numeric values

order_items_data['price'] = pd.to_numeric(order_items_data['price'])
order_items_data['shipping_charges'] = pd.to_numeric(order_items_data['shipping_charges'])

#--------------------------------------------------------------------------------------------------
#adding total price
order_items_data['total_charges'] = order_items_data['price'] + order_items_data['shipping_charges']

#---------------------------------------------------------------------------------------------------
# the updated record is saving in a new file
new_order_items_data = "../transform_data/updated_data/OrderItems.csv"
order_items_data.to_csv(new_order_items_data,index=False)
