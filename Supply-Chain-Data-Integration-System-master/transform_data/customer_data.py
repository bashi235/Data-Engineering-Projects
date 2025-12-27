import pandas as pd

customer_data = pd.read_csv("../data_resources/Customers.csv")

# converting the customer_zip_code_prefix to numeric data
customer_data['customer_zip_code_prefix'] = pd.to_numeric(customer_data['customer_zip_code_prefix'])

# to find the null values in the record
print(customer_data.isnull().sum())
# the customers data showing 0 null values so the customer records is almost everything  is cleaned

# the updated record is saving in a new file
new_customer_data = "../transform_data/updated_data/Customers.csv"
customer_data.to_csv(new_customer_data,index=False)





