# 🏗️ SQL Data Warehouse Project

## 📌 Overview

This project represents a **Data Warehouse (DW) system** built using a layered architecture approach. The goal is to transform raw data into clean, structured, and analytics-ready datasets.

The warehouse is organized into three main layers:

* **Bronze (Raw Layer)**
* **Silver (Cleaned & Transformed Layer)**
* **Gold (Business Layer)**

---

## 🧠 What is a Data Warehouse?

A **Data Warehouse** is a centralized system used for storing, transforming, and analyzing data from multiple sources. It is optimized for:

* Reporting 📊
* Analytics 📈
* Business Intelligence (BI)

Unlike transactional systems (OLTP), a DW is designed for **read-heavy operations and historical data analysis**.

---

## 🏗️ Architecture

```
Source Systems → Bronze → Silver → Gold → BI / Analytics
```

---

## 🥉 Bronze Layer (Raw Data)

### 📌 Purpose:

* Stores raw data exactly as it comes from source systems
* No transformations applied

### ⚙️ Characteristics:

* Data loaded using `BULK INSERT`
* Tables truncated and reloaded
* Keeps original structure

### 📂 Example Tables:

* `crm_cust_info`
* `crm_prd_info`
* `crm_sales_details`
* `erp_cust_az12`
* `erp_loc_a101`
* `erp_px_cat_g1v2`

---

## 🥈 Silver Layer (Cleaned Data)

### 📌 Purpose:

* Cleans and standardizes data from Bronze
* Applies business rules and transformations

### ⚙️ Transformations:

* Removing duplicates
* Handling NULL values
* Data type conversions
* Standardizing values (e.g. gender, country)
* Fixing inconsistent data

### 💡 Example:

* `'M' → 'Male'`
* `'F' → 'Female'`
* `NULL → 'n/a'`

---

## 🥇 Gold Layer (Business Ready Data)

### 📌 Purpose:

* Provides data ready for reporting and analytics
* Optimized for business users

### ⚙️ Characteristics:

* Aggregated data
* Business metrics (KPIs)
* Fact and dimension tables

### 📊 Example Use Cases:

* Sales reporting
* Customer analytics
* Product performance

---

## ⚙️ ETL Process

The project uses SQL stored procedures for ETL:

### 🔄 Bronze Load

* Loads raw data from CSV files into database tables

### 🔄 Silver Load

* Transforms and cleans data from Bronze

### 🔄 Gold Load (optional)

* Aggregates and prepares data for reporting

---

## 📁 Project Structure

```
sql-data-warehouse-project/
│
├── data/                # Source datasets (CSV files)
├── docs/                # Documentation
├── scripts/
│   ├── bronze/
│   │   └── load_bronze.sql
│   │
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   └── load_silver.sql
│   │
│   └── gold/
│       └──ddl_gold.sql
│
├── tests/               # Data quality tests
├── README.md
```

---

## 🚀 How to Run

1. Create database
2. Run DDL scripts
3. Execute Bronze procedure:

   ```sql
   exec bronze.load_bronze;
   ```
4. Execute Silver procedure:

   ```sql
   exec silver.load_silver;
   ```

---

## 🧪 Data Quality

Basic checks implemented:

* Removing duplicates
* Handling invalid dates
* Recalculating incorrect values
* Standardizing text fields

---

## 📈 Future Improvements

* Add Gold layer (fact/dimension modeling)
* Implement incremental loads
* Add logging tables instead of PRINT
* Integrate with BI tools (Power BI, Tableau)

---

## 👨‍💻 Author

Lazar Vidic

---

## 📄 License

This project is open-source and available under the MIT License.
