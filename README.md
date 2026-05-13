# Business Intelligence Platform

## Project Overview

A complete end-to-end data platform that automatically scrapes data from websites, cleans and transforms it, loads it into a MySQL data warehouse, trains machine learning models, and displays insights on an interactive dashboard. The entire pipeline runs automatically with email alerts.

## System Architecture
Website (books.toscrape.com)
|
| Scraper (run_scraper.py --auto)
v
data/raw/books_YYYYMMDD_HHMMSS.csv
|
| ETL (run_etl.py)
v
MySQL Database (business_intelligence)
|
| Data Warehouse (populate_data_warehouse.py)
v
dim_book + dim_rating + dim_date + dim_price_category + fact_book_analysis
|
| ML Models (01-04_train_*.py)
v
Price Predictor Models
|
| Dashboard (streamlit run dashboard.py)
v
Interactive Web Dashboard at http://localhost:8501

text

## What This Project Does

| Step | Action | Output |
|------|--------|--------|
| 1 | Scrapes 60 books from Books to Scrape | CSV files in data/raw/ |
| 2 | Cleans and transforms data | Cleaned CSV in data/processed/ |
| 3 | Loads into MySQL database | 60 books in books table |
| 4 | Creates star schema warehouse | 5 dimension tables + 1 fact table |
| 5 | Trains 4 ML models | Model files in 04_data_science/models/ |
| 6 | Compares models and picks best | best_model.txt |
| 7 | Displays interactive dashboard | http://localhost:8501 |
| 8 | Generates output files | CSV, report, HTML in output/ |
| 9 | Automates daily runs | Scheduler runs at 2 AM |
| 10 | Sends email alerts | Success/failure notifications |

## Project Structure
business_intelligence_platform/
│
├── 01_data_collection/ # Web scraping modules
│ ├── fetch_page.py # Handles HTTP requests
│ ├── parse_books.py # Extracts data from HTML
│ ├── save_data.py # Saves to CSV files
│ ├── scrape_all.py # Orchestrates scraping
│ └── run_scraper.py # Main scraper entry point
│
├── 02_data_engineering/ # ETL pipeline
│ ├── read_data.py # Reads CSV files
│ ├── clean_data.py # Cleans missing values
│ ├── transform_data.py # Converts data types
│ ├── validate_data.py # Data quality checks
│ ├── load_to_mysql.py # Loads to database
│ ├── run_etl.py # Main ETL entry point
│ └── update_outputs/ # Output generation module
│ ├── data_loader.py
│ ├── csv_exporter.py
│ ├── report_generator.py
│ ├── html_dashboard.py
│ └── run_updates.py
│
├── 03_data_warehouse/ # Star schema design
│ ├── create_dimension_tables.py
│ ├── create_fact_table.py
│ └── populate_data_warehouse.py
│
├── 04_data_science/ # Machine learning
│ ├── 01_train_linear_models.py
│ ├── 02_train_decision_tree.py
│ ├── 03_train_random_forest.py
│ ├── 04_train_gradient_boosting.py
│ ├── 05_compare_all_models.py
│ ├── 06_predict_with_best.py
│ └── models/ # Saved model files
│
├── 05_analytics_dashboard/ # Streamlit dashboard
│ ├── dashboard.py
│ ├── data_loader.py
│ ├── charts.py
│ └── filters.py
│
├── 06_pipeline_orchestration/ # Automation
│ ├── scheduler.py
│ ├── email_alerts.py
│ ├── master_pipeline.py
│ └── setup_windows_scheduler.bat
│
├── data/ # Data storage
│ ├── raw/ # Original scraped CSV
│ ├── processed/ # Cleaned CSV
│ └── warehouse/ # Database backups
│
├── output/ # Generated outputs
│ ├── exports/ # CSV exports
│ ├── reports/ # Text reports
│ └── dashboards/ # HTML dashboards
│
├── logs/ # Execution logs
├── config.py # Configuration settings
├── requirements.txt # Python dependencies
├── run_everything.py # Complete pipeline runner
└── README.md # This file

text

## Prerequisites

| Requirement | Version | Check Command |
|-------------|---------|---------------|
| Python | 3.9+ | `python --version` |
| MySQL | 8.0+ | `mysql --version` |
| pip | Latest | `pip --version` |

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone https://github.com/George-tech-svg/business-intelligence-platform.git
cd business-intelligence-platform
Step 2: Install Python Packages
bash
pip install -r requirements.txt
Step 3: Set Up MySQL Database
Connect to MySQL:

bash
mysql -u root -p
Run these SQL commands:

sql
CREATE DATABASE business_intelligence;
CREATE USER 'analyst'@'localhost' IDENTIFIED BY 'analyst123';
GRANT ALL PRIVILEGES ON business_intelligence.* TO 'analyst'@'localhost';
FLUSH PRIVILEGES;
EXIT;
Step 4: Configure config.py
Copy the example config and add your details:

bash
cp config.py.example config.py
Edit config.py with your database password and email settings.

Step 5: Create Data Warehouse Tables
bash
cd 03_data_warehouse
python create_dimension_tables.py
python create_fact_table.py
cd ..
Step 6: Test Database Connection
bash
python test_connection.py
How to Run
One Command - Run Everything
bash
python run_everything.py
This runs: Scraper -> ETL -> Data Warehouse -> ML Models -> Outputs -> Dashboard

Individual Commands
Task	Command
Scrape only	cd 01_data_collection && python run_scraper.py --auto
ETL only	cd 02_data_engineering && python run_etl.py
Train Linear Models	cd 04_data_science && python 01_train_linear_models.py
Train Decision Tree	cd 04_data_science && python 02_train_decision_tree.py
Train Random Forest	cd 04_data_science && python 03_train_random_forest.py
Train Gradient Boosting	cd 04_data_science && python 04_train_gradient_boosting.py
Compare all models	cd 04_data_science && python 05_compare_all_models.py
Live dashboard	cd 05_analytics_dashboard && streamlit run dashboard.py
Full pipeline	cd 06_pipeline_orchestration && python master_pipeline.py --now
Output Files
After each ETL run, the following files are automatically generated:

File	Location	Description	Open With
books_export_latest.csv	output/exports/	Spreadsheet of all books	Excel, Google Sheets
summary_report_latest.txt	output/reports/	Text summary report	Notepad, any text editor
dashboard_latest.html	output/dashboards/	Static HTML dashboard	Chrome, Edge, Firefox
Machine Learning Models
Four models are trained and compared:

Model	MAE (Average Error)	R² Score
Lasso Regression	$15.02	-0.1094
Gradient Boosting	$15.45	-0.2006
Decision Tree	$16.11	-0.3692
Random Forest	$16.15	-0.3732
The best model is saved to 04_data_science/models/best_model.txt

Data Warehouse Schema
Dimension Tables
Table	Purpose	Columns
dim_book	Book information	book_id, book_title, original_price
dim_rating	Rating lookup	rating_id, rating_name, rating_number, rating_category
dim_date	Date information	date_id, full_date, year, month, day
dim_price_category	Price categories	category_id, category_name, min_price, max_price
Fact Table
Table	Purpose	Columns
fact_book_analysis	Connects all dimensions	fact_id, book_id, rating_id, date_id, category_id, price_numeric
Dashboard Features
Feature	Description
Key Metrics	Total books, average price, price range, average rating
Price Distribution	Histogram showing book price spread
Rating Distribution	Bar chart of 1-5 star ratings
Price by Rating	Line chart showing trend
Price Category Pie Chart	Budget, Mid, Premium breakdown
Top 10 Expensive Books	List with prices and ratings
Best Value Books	High-rated books with low prices
Sidebar Filters	Filter by rating, price category, price range
Automation
Manual Daily Run
bash
python run_everything.py
Scheduled Daily Run (Windows)
Run as Administrator:

bash
cd 06_pipeline_orchestration
setup_windows_scheduler.bat
Email Alerts
Configure email in config.py using a Gmail App Password.

Database Backup
Create Backup
bash
cmd
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe" -u analyst -p business_intelligence --no-tablespaces > data\warehouse\backup.sql
exit
Restore from Backup
bash
cmd
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u analyst -p business_intelligence < data\warehouse\backup.sql
exit
Common Errors and Fixes
Error	Solution
mysql command not found	Use full path to mysql.exe
Access denied for user	Check password in config.py
Module not found	Run pip install -r requirements.txt
Scraper asks for input	Use --auto flag
Port 8501 already in use	Use --server.port 8502
Can't connect to MySQL	Run net start MySQL80 as admin
Technologies Used
Category	Technologies
Languages	Python 3.14, SQL
Web Scraping	BeautifulSoup4, Requests
Data Processing	Pandas, NumPy
Database	MySQL 8.0, SQLAlchemy
Machine Learning	Scikit-learn, XGBoost, Joblib
Visualization	Streamlit, Plotly, Matplotlib
Automation	Schedule, SMTP, Windows Task Scheduler
Skills Demonstrated
Web scraping with rate limiting and retry logic

ETL pipeline design and implementation

Star schema data warehouse design

Machine learning model training and comparison

Interactive dashboard development

Pipeline automation and scheduling

Email alert system integration

Production-ready code structure

Project Status
Phase	Component	Status
Phase 1	Web Scraper	Complete
Phase 2	ETL Pipeline	Complete
Phase 3	Data Warehouse	Complete
Phase 4	Machine Learning	Complete
Phase 5	Analytics Dashboard	Complete
Phase 6	Automation	Complete
