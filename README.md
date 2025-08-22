# 📦 Amazon Orders Data Warehouse

## 🚀 Project Overview
This project is a **data warehousing solution** built on SQL Server for analyzing Amazon order data.  
The schema follows a **Star Schema** design with one central fact table and multiple supporting dimension tables.  

The warehouse enables:
- Clean & consistent order data
- Fast analytical queries with indexed fact tables
- Structured reporting (e.g., sales by month, top products, customer locations, etc.)

---

## 🏗 Schema Design

### ⭐ Star Schema
- **Fact Table** → `fact_order_lines`  
  Stores transactional details like order, product, quantity, and revenue.

- **Dimension Tables**:  
  - `dim_date` → calendar attributes (year, month, week, quarter, etc.)  
  - `dim_currency` → currency codes  
  - `dim_status` → order + courier status  
  - `dim_channel` → sales channel & fulfillment details  
  - `dim_customer_location` → customer shipping details  
  - `dim_product` → product-level attributes (ASIN, SKU, style, category, size)

---

## ⚙️ ETL Pipeline

1. **Staging Table**  
   - Raw data loaded via `BULK INSERT` into `staging_orders`.

2. **Cleaning View** (`v_orders_clean`)  
   - Standardizes dates, trims strings, fixes nulls.

3. **Dimension Population**  
   - Inserts distinct values into each `dim_*` table.  
   - Ensures uniqueness with constraints.

4. **Fact Table Population**  
   - Joins cleaned data with dimension keys.  
   - Stores final analytic-ready transactions.

---

## 📂 Repository Structure
```
├── 01_staging.sql          # staging table + bulk load
├── 02_views.sql            # data cleaning view
├── 03_dimensions.sql       # dimension tables
├── 04_fact.sql             # fact table creation
├── 05_indexes.sql          # indexes for performance
├── sample_data/            # raw CSV (if included)
└── README.md               # documentation
```

---

## 📊 Example Queries

### Monthly Sales
```sql
SELECT d.year, d.month, SUM(f.amount) AS total_sales
FROM fact_order_lines f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

### Top Products
```sql
SELECT p.sku, SUM(f.amount) AS revenue
FROM fact_order_lines f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.sku
ORDER BY revenue DESC
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
```

---

## 🛠 Tech Stack
- **SQL Server (T-SQL)**  
- **Data Warehousing Concepts** (Star Schema, Dimensions, Facts)  
- **GitHub** for version control  

---

## 📌 Future Scope
- Implement Slowly Changing Dimensions (SCD)  
- Automate ETL with Python or SSIS  
- Build dashboards in **Power BI / Tableau**  

---

## 👨‍💻 Author
**Your Name**  
📧 your.email@example.com  
🔗 [LinkedIn](https://linkedin.com/in/your-profile) | [GitHub](https://github.com/your-username)
