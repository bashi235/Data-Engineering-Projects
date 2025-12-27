# 📦 Supply Chain Data Integration System

## 📘 Abstract
This project integrates and analyzes supply chain data from an E-commerce **Order & Supply Chain** dataset to generate actionable operational insights. Using **PySpark**, the system builds a complete ETL pipeline that performs data cleaning, transformation, and computation of essential logistics metrics such as **lead time, order cycle duration, seller performance, and shipping efficiency**. The unified dataset enables automated analysis and visualization of supply chain behavior across product categories and vendor networks.

---

## 📝 Project Overview
This project focuses on integrating and analyzing supply chain data from an E-commerce marketplace dataset containing information about **customers, orders, products, sellers, and order items**.  
The goal is to study operational performance across product categories and vendors by examining:

- Lead time  
- Order cycle efficiency  
- Profitability  
- Shipping performance  
- Inventory movement  

Using **PySpark** as the processing engine, the project cleans raw CSV files, standardizes formats, computes shipping-related KPIs, and merges them into a consolidated dataset. Additional metrics—such as seller profit, shipping efficiency, and category-wise revenue—provide deep insights into **demand patterns, slow-moving inventory, and vendor contribution**.  
These outputs are exported for visualization, supporting data-driven decisions in procurement, logistics planning, vendor management, and supply chain optimization.

---

## 🎯 Objectives
- Integrate multi-file e-commerce supply chain data into a unified analytical dataset  
- Compute key operational metrics such as **lead time, order cycle time, seller profit, shipping efficiency, and category revenue**  
- Analyze inventory behavior to identify slow-moving and fast-moving products  
- Generate insights across categories, sellers, and regions for operational decision-making  
- Visualize results using optimized **Matplotlib** charts for clear communication  

---

## 🛠 Tools and Technologies Used
- **Programming Language:** Python  
- **Data Processing:** PySpark (Spark SQL, DataFrames)  
- **IDE:** PyCharm  
- **Visualization:** Matplotlib  
- **Libraries:** Pandas, Matplotlib  
- **Environment Setup:** Java JDK 17, Spark 3.5.7  

---

# 🔄 ETL (Extract, Transform, Load)

## 📥 Extract
The project uses an E-commerce Order & Supply Chain dataset including customers, orders, order items, and product details.  
All datasets were loaded into PySpark as distributed DataFrames and connected using relational keys such as **order_id**, **customer_id**, **product_id**, and **seller_id**.  
This integration provides a complete view of the supply chain from order placement to shipping, vendor involvement, pricing, and product characteristics.

---

## 🔧 Transform
Using PySpark, extensive cleaning and preprocessing steps were performed, including:

- Handling missing values  
- Enforcing schema consistency  
- Standardizing date formats  
- Converting numeric fields  
- Removing duplicates  
- Normalizing location data  

Key operational metrics computed:

- Lead time  
- Order cycle time  
- Seller cost  
- Seller profit  
- Shipping efficiency  
- Product volume and weight categories  

Additional enriched fields created:

- Customer full location  
- Product classification  

---

## 📤 Load
The cleaned and transformed datasets were merged into a unified PySpark DataFrame representing the complete supply chain workflow.  
Analytical outputs were exported as CSV files, including:

- Lead time  
- Order cycle time  
- Seller performance  
- Category-wise revenue  
- Inventory distribution  
- State-wise order volume  

Visualizations were generated using **Matplotlib** and exported as PNG charts for reporting and dashboard use.

---

# 🔍 Key Insights

### 1️⃣ Average Lead Time by Product Category
- Measures days between order placement and approval  
- Identifies product categories with longer approval delays and operational bottlenecks  

### 2️⃣ Average Order Cycle Time by Product Category
- Measures total time from order placement to shipment (with simulated transit days)  
- Highlights delays in fulfillment or seller processing  

### 3️⃣ Shipping Efficiency by Seller
Formula:
- Identifies top-performing and low-performing sellers  
- Helps determine pricing and sourcing strategies  

### 5️⃣ Seller Contribution (Top Vendors by Revenue)
Identifies:

- Top revenue-generating sellers  
- Low-performing vendors  
- Sales distribution across vendor network  

Useful for vendor management and performance benchmarking.

### 6️⃣ Category-wise Total Revenue
- Aggregated revenue per product category  
- Identifies best-selling and low-revenue categories  
- Supports procurement and demand forecasting  

### 7️⃣ State-wise Order Volume Distribution
- Order count per customer_state  
- Highlights high-demand regions and potential growth areas  
- Helps optimize logistics network  

---

# 📈 Visualization
Using **Matplotlib**, the project includes clear visualizations such as:

- Horizontal bar charts for lead time and order cycle time  
- Bar, lollipop, and pie charts for seller performance and contribution  
- Line charts for category-wise revenue and state-wise order volume  

These visualizations reveal fulfillment delays, high-performing vendors, top product categories, and regional demand patterns.

---

