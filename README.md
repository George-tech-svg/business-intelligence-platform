# Business Intelligence Platform

## Project Overview

A complete end-to-end Business Intelligence and Data Engineering platform that automatically:

- Scrapes data from websites
- Cleans and transforms raw data
- Loads data into a MySQL data warehouse
- Trains Machine Learning models
- Generates reports and exports
- Displays insights on an interactive dashboard
- Automates the entire pipeline with scheduling and email alerts

---

# System Architecture

```text
Website (books.toscrape.com)
            │
            ▼
Scraper (run_scraper.py --auto)
            │
            ▼
data/raw/books_YYYYMMDD_HHMMSS.csv
            │
            ▼
ETL Pipeline (run_etl.py)
            │
            ▼
MySQL Database (business_intelligence)
            │
            ▼
Data Warehouse (populate_data_warehouse.py)
            │
            ▼
dim_book + dim_rating + dim_date
+ dim_price_category + fact_book_analysis
            │
            ▼
Machine Learning Models
(01-04_train_*.py)
            │
            ▼
Price Prediction Models
            │
            ▼
Analytics Dashboard
(streamlit run dashboard.py)
            │
            ▼
Interactive Dashboard:
http://localhost:8501
```

---

# Key Features

| Feature | Description |
|---|---|
| Automated Web Scraping | Scrapes book data from Books to Scrape |
| ETL Pipeline | Cleans, validates, and transforms raw data |
| MySQL Integration | Loads processed data into a relational database |
| Data Warehouse | Implements a star schema warehouse |
| Machine Learning | Trains and compares multiple ML models |
| Analytics Dashboard | Interactive Streamlit dashboard |
| Automation | Daily scheduled pipeline execution |
| Email Alerts | Success and failure notifications |
| Reporting | Generates CSV, HTML, and text reports |

---

# Technologies Used

| Category | Technologies |
|---|---|
| Programming Language | Python 3 |
| Web Scraping | BeautifulSoup4, Requests |
| Data Processing | Pandas, NumPy |
| Database | MySQL, SQLAlchemy |
| Machine Learning | Scikit-learn, XGBoost, Joblib |
| Dashboard | Streamlit, Plotly, Matplotlib |
| Automation | Schedule, SMTP, Windows Task Scheduler |

---

# Project Structure

```text
business_intelligence_platform/
│
├── 01_data_collection/
│   ├── fetch_page.py
│   ├── parse_books.py
│   ├── save_data.py
│   ├── scrape_all.py
│   └── run_scraper.py
│
├── 02_data_engineering/
│   ├── read_data.py
│   ├── clean_data.py
│   ├── transform_data.py
│   ├── validate_data.py
│   ├── load_to_mysql.py
│   ├── run_etl.py
│   └── update_outputs/
│
├── 03_data_warehouse/
│   ├── create_dimension_tables.py
│   ├── create_fact_table.py
│   └── populate_data_warehouse.py
│
├── 04_data_science/
│   ├── 01_train_linear_models.py
│   ├── 02_train_decision_tree.py
│   ├── 03_train_random_forest.py
│   ├── 04_train_gradient_boosting.py
│   ├── 05_compare_all_models.py
│   ├── 06_predict_with_best.py
│   └── models/
│
├── 05_analytics_dashboard/
│   ├── dashboard.py
│   ├── data_loader.py
│   ├── charts.py
│   └── filters.py
│
├── 06_pipeline_orchestration/
│   ├── scheduler.py
│   ├── email_alerts.py
│   ├── master_pipeline.py
│   └── setup_windows_scheduler.bat
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── warehouse/
│
├── output/
│   ├── exports/
│   ├── reports/
│   └── dashboards/
│
├── logs/
├── config.py
├── requirements.txt
├── run_everything.py
└── README.md
```

---

# Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.9+ |
| MySQL | 8.0+ |
| pip | Latest |

---

# Installation and Setup

## 1. Clone the Repository

```bash
git clone https://github.com/George-tech-svg/business-intelligence-platform.git

cd business-intelligence-platform
```

---

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Set Up MySQL Database

Open MySQL:

```bash
mysql -u root -p
```

Run the following SQL commands:

```sql
CREATE DATABASE business_intelligence;

CREATE USER 'analyst'@'localhost'
IDENTIFIED BY 'analyst123';

GRANT ALL PRIVILEGES ON business_intelligence.*
TO 'analyst'@'localhost';

FLUSH PRIVILEGES;

EXIT;
```

---

## 4. Configure the Application

```bash
cp config.py.example config.py
```

Update `config.py` with:

- Database credentials
- Email configuration
- Pipeline settings

---

## 5. Create Data Warehouse Tables

```bash
cd 03_data_warehouse

python create_dimension_tables.py

python create_fact_table.py

cd ..
```

---

## 6. Test Database Connection

```bash
python test_connection.py
```

---

# Running the Project

## Run Entire Pipeline

```bash
python run_everything.py
```

This automatically runs:

```text
Scraper
   ↓
ETL Pipeline
   ↓
Data Warehouse
   ↓
Machine Learning
   ↓
Report Generation
   ↓
Dashboard
```

---

# Individual Commands

| Task | Command |
|---|---|
| Scrape Data | `cd 01_data_collection && python run_scraper.py --auto` |
| Run ETL | `cd 02_data_engineering && python run_etl.py` |
| Train Linear Models | `cd 04_data_science && python 01_train_linear_models.py` |
| Train Decision Tree | `cd 04_data_science && python 02_train_decision_tree.py` |
| Train Random Forest | `cd 04_data_science && python 03_train_random_forest.py` |
| Train Gradient Boosting | `cd 04_data_science && python 04_train_gradient_boosting.py` |
| Compare Models | `cd 04_data_science && python 05_compare_all_models.py` |
| Launch Dashboard | `cd 05_analytics_dashboard && streamlit run dashboard.py` |
| Run Full Pipeline | `cd 06_pipeline_orchestration && python master_pipeline.py --now` |

---

# Machine Learning Models

The platform trains and compares four Machine Learning models:

| Model | MAE | R² Score |
|---|---|---|
| Lasso Regression | 15.02 | -0.1094 |
| Gradient Boosting | 15.45 | -0.2006 |
| Decision Tree | 16.11 | -0.3692 |
| Random Forest | 16.15 | -0.3732 |

The best-performing model is automatically saved to:

```text
04_data_science/models/best_model.txt
```

---

# Data Warehouse Schema

## Dimension Tables

| Table | Purpose |
|---|---|
| dim_book | Stores book information |
| dim_rating | Rating lookup table |
| dim_date | Date dimension |
| dim_price_category | Price categorization |

## Fact Table

| Table | Purpose |
|---|---|
| fact_book_analysis | Connects all dimensions for analytics |

---

# Dashboard Features

| Feature | Description |
|---|---|
| Key Metrics | Total books, average price, ratings |
| Price Distribution | Histogram visualization |
| Rating Distribution | Star rating analysis |
| Price by Rating | Trend analysis |
| Category Breakdown | Budget, Mid, Premium |
| Top Expensive Books | Ranked price table |
| Best Value Books | High-rated low-cost books |
| Sidebar Filters | Dynamic filtering |

---

# Output Files

After each ETL run, the platform automatically generates:

| File | Purpose |
|---|---|
| books_export_latest.csv | Spreadsheet export |
| summary_report_latest.txt | Summary report |
| dashboard_latest.html | Static HTML dashboard |

---

# Automation

## Manual Run

```bash
python run_everything.py
```

---

## Scheduled Daily Run (Windows)

```bash
cd 06_pipeline_orchestration

setup_windows_scheduler.bat
```

---

# Database Backup

## Create Backup

```bash
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe" ^
-u analyst -p business_intelligence --no-tablespaces ^
> data\warehouse\backup.sql
```

---

## Restore Backup

```bash
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" ^
-u analyst -p business_intelligence ^
< data\warehouse\backup.sql
```

---

# Common Errors and Solutions

| Error | Solution |
|---|---|
| mysql command not found | Use full path to mysql.exe |
| Access denied for user | Verify credentials in config.py |
| Module not found | Run `pip install -r requirements.txt` |
| Scraper asks for input | Use `--auto` flag |
| Port 8501 already in use | Change Streamlit port |
| Cannot connect to MySQL | Start MySQL service |

---

# Skills Demonstrated

- Web scraping and automation
- ETL pipeline engineering
- Data cleaning and validation
- Relational database design
- Star schema data warehouse design
- Machine Learning model training
- Model evaluation and comparison
- Dashboard development with Streamlit
- Data visualization
- Pipeline orchestration
- Scheduling and automation
- Email notification systems
- Production-ready project structure

---

# Project Status

| Phase | Component | Status |
|---|---|---|
| Phase 1 | Web Scraper | Complete |
| Phase 2 | ETL Pipeline | Complete |
| Phase 3 | Data Warehouse | Complete |
| Phase 4 | Machine Learning | Complete |
| Phase 5 | Analytics Dashboard | Complete |
| Phase 6 | Automation | Complete |

---

# Author

George Baji

GitHub:
https://github.com/George-tech-svg
