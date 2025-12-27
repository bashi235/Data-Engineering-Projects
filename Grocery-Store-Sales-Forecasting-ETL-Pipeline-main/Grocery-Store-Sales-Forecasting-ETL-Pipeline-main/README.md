
---

## 📂 Dataset Description
Dataset: **Store Sales — Time Series Forecasting (Corporación Favorita)**  
Source: Kaggle

**Files Used**
- `train.csv` – historical sales data
- `transactions.csv` – customer traffic
- `stores.csv` – store metadata
- `oil.csv` – oil prices
- `holidays_events.csv` – holiday calendar


---

## 🥉 Bronze Layer – Raw Ingestion
- Implemented using **Delta Live Tables**
- Loads raw CSV files directly from S3
- Applies minimal transformations (schema inference, date casting)
- Acts as the **immutable source of truth**

**Output Tables**
- `sales_bronze`
- `transactions_bronze`
- `stores_bronze`
- `holidays_bronze`
- `oils_bronze`

---

## 🥈 Silver Layer – Cleaning & Enrichment
- Cleans and validates Bronze data
- Removes duplicates
- Fixes invalid sales values
- Forward-fills missing oil prices using window functions
- Joins all datasets into a single enriched fact table

**Key Features**
- DLT Expectations for data quality
- Error records captured using expectations
- Unified table: `merged_sales`

**Error Logging**
- Rejected records are stored in : grocery_catalog.logs.etl_errors


---

## 🥇 Gold Layer 
Transforms clean Silver data into **business-ready tables**.

**Gold Tables**
- `daily_sales`
- `weekly_sales`
- `store_performance`
- `family_performance`

## 🔄 Orchestration & Automation
- Pipeline deployed using **Databricks DLT**
- Scheduled via **Databricks Workflows**
- Runs **daily at 4:00 AM UTC**
- Configured with:
  - Automatic retries
  - Email alerts for success/failure
  - Centralized logging

---

## 📊 Data Visualization
Basic exploratory visualizations created using **Pandas + Matplotlib**:
- Daily sales trends
- Weekly sales per store
- Top-performing stores
- Category (family) performance

---

## ✅ Key Outcomes
- End-to-end automated ETL pipeline
- High-quality, governed datasets
- Analytics-ready Gold layer
- Production-ready monitoring and alerts

---
 
