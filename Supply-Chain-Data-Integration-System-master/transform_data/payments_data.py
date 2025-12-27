import pandas as pd

payments_data = pd.read_csv("../data_resources/Payments.csv")

# print(payments_data.isnull().sum())
# we got no nan values

# changing payment_sequential, payment_installments, payment_value
#
payments_data['payment_sequential'] = pd.to_numeric(payments_data['payment_sequential'])
payments_data['payment_installments'] = pd.to_numeric(payments_data['payment_installments'])
payments_data['payment_value'] = pd.to_numeric(payments_data['payment_value'])

#-----------------------------------------------------------------------------------------------

#checking if any duplicate order_ids available if so we need to group them
duplicate_orders = payments_data['order_id'].value_counts()
duplicate_orders = duplicate_orders[duplicate_orders > 1]
print(duplicate_orders)
# since we get 0 duplicate values we no need to perform any grouping of order_id
# payment_group = payments_data.groupby("order_id").agg({
#     'payment_value' : 'sum',
#     'payment_installments' : 'max',
#     'payment_type' : 'first',
# }).reset_index()


# the updated record is saving in a new file
new_payments_data = "../transform_data/updated_data/Payments.csv"
payments_data.to_csv(new_payments_data,index=False)




