# 🚢 Trade Flow Brasil

## 📌 Overview

Trade Flow Brasil is an end-to-end Data Engineering project that analyzes Brazil's import and export flows using official public trade data APIs.

The project applies modern data architecture principles, including layered data processing (RAW, BRONZE, GOLD), data quality validation, and cloud-based data warehousing using Google Cloud Platform (GCP).

The final dataset enables strategic analysis of Brazil’s trade balance, top exported/imported products, and international trade relationships.

---

## 🎯 Project Objectives

- Identify Brazil’s main export and import products
- Build a scalable data pipeline using public API data
- Apply layered data architecture (Data Lake approach)
- Implement data quality validations
- Serve curated analytical datasets in a cloud data warehouse
- Enable business-level insights from raw governmental data

---

## 🏗️ Architecture

The project follows a layered architecture model:

### 🔹 RAW Layer (Landing Zone)
- Data extracted directly from public trade APIs
- Stored without transformation
- Historical data preserved
- Stored in Google Cloud Storage

### 🔹 BRONZE Layer (Standardized Data)
- Column standardization
- Data type enforcement
- Null handling
- Deduplication
- Basic normalization

### 🔹 GOLD Layer (Analytics Ready)
- Dimensional modeling
- Fact table: Trade transactions
- Dimension tables:
  - Product (NCM)
  - Country
  - Time
  - Trade Type (Export / Import)

- Stored in BigQuery

---

## 🛠️ Tech Stack

- Python
- Pandas
- Google Cloud Storage
- BigQuery
- Cloud Functions / Cloud Composer (Airflow)
- SQL
- Data Quality Validation Scripts

---

## 🔄 Data Pipeline Flow

1. Extract data from public trade API
2. Store raw data in Cloud Storage (RAW)
3. Transform and clean data (BRONZE)
4. Apply data quality rules
5. Load curated tables into BigQuery (GOLD)
6. Enable analytical queries and dashboards

---

## 🧪 Data Quality Rules

Examples of validations implemented:

- Negative trade values check
- Invalid or null NCM codes
- Missing country information
- Duplicate records detection
- Date consistency validation

All validation logs can be stored for monitoring and governance purposes.

---

## 📊 Analytical Use Cases

- Top 10 exported products by value
- Top importing countries
- Trade balance by month
- Year-over-Year growth analysis
- Sector performance over time

---

## 📈 Future Improvements

- Incremental loading strategy
- Partitioned and clustered BigQuery tables
- Automated orchestration with Airflow
- Dashboard integration (Looker Studio / Power BI)
- Data observability layer
- Unit testing for data pipeline

---

## 🧠 Why This Project?

This project demonstrates:

- Data Engineering best practices
- Cloud architecture implementation
- Data governance and quality controls
- Dimensional modeling
- API data ingestion
- Business-oriented data transformation

---

## 👨‍💻 Author

João Gabriel  
Data & Analytics Enthusiast  
Focused on Data Engineering, Cloud Architecture and Strategic Analytics
