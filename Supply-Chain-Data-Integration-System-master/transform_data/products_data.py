import pandas as pd

# checking the null values
products_data = pd.read_csv("../data_resources/Products.csv")

# converting the string data to numberic data : product_weight_g,product_length_cm,product_height_cm,product_widhth_cm
products_data['product_weight_g'] = pd.to_numeric(products_data['product_weight_g'],errors='coerce')
products_data['product_length_cm'] = pd.to_numeric(products_data['product_length_cm'],errors='coerce')
products_data['product_height_cm'] = pd.to_numeric(products_data['product_height_cm'],errors='coerce')
products_data['product_width_cm'] = pd.to_numeric(products_data['product_width_cm'],errors='coerce')

#----------------------------------------------------------------------------------------------------------------------
# print(products_data.isnull().sum())
# product_id                 0
# product_category_name    168
# product_weight_g          10
# product_length_cm         10
# product_height_cm         10
# product_width_cm          10
# these are the null attributes

# managing nan values

# managing nan values of product_category_name
products_data['product_category_name'] = products_data['product_category_name'].fillna('Unknown')


#managing nan values of product_weight_g,product_length_cm,product_height_cm,product_width_cm
# for that replacing the nan values in these attributes ; considering the mean values of them

# product_weight_g
weight_mean = products_data['product_weight_g'].mean()
weight_mean = round(weight_mean,2)
products_data['product_weight_g'] = products_data['product_weight_g'].fillna(weight_mean)

# product_length_cm
length_mean = products_data['product_length_cm'].mean()
length_mean = round(length_mean,2)
products_data['product_length_cm'] = products_data['product_length_cm'].fillna(length_mean)

# product_height_cm
height_mean = products_data['product_height_cm'].mean()
height_mean = round(height_mean,2)
products_data['product_height_cm'] = products_data['product_height_cm'].fillna(height_mean)

#product_width_cm
width_mean = products_data['product_width_cm'].mean()
width_mean = round(width_mean,2)
products_data['product_width_cm'] = products_data['product_width_cm'].fillna(width_mean)

#print(products_data.isnull().sum())
#---------------------------------------------------------------------------------------------------------------------

# adding the volume and the weight category to the products_data

#adding volume attribute
products_data['product_volume_cm3']  = products_data['product_length_cm'] * products_data['product_width_cm'] * products_data['product_height_cm']

# adding the weight category [micro, small, medium, heavy, extreme]
#print(products_data['product_weight_g'].max()) - 30kg
# weight division :
# micro : 0 to 500 g
# small : 500 to 2000g
# medium : 2000g to 7000g
# heavy : 7000g to 15000g
# extreme : 15000g to 30000g

weight_category = ['micro','small','medium','heavy','extreme']
max_weight = products_data['product_weight_g'].max()
weight_division = [0, 500,2000,7000,15000,max_weight+1]

products_data['product_weight_category'] = pd.cut(products_data['product_weight_g'],weight_division,labels=weight_category,right=False)

#---------------------------------------------------------------------------------------------------------------------

# the updated record is saving in a new file
new_products_data = "../transform_data/updated_data/Products.csv"
products_data.to_csv(new_products_data,index=False)







