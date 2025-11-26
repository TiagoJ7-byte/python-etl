# python-etl

This project implements an ETL (Extract, Transform, Load) pipeline in Python to collect weather data from a public API, process it, and persist it into a PostgreSQL database for further analysis and reporting.

⚙️ Tech Stack

 - Python: Core language for ETL orchestration

 - Requests: For API calls to fetch weather data

 - Pandas: For data transformation and cleaning

 - SQLAlchemy / psycopg2: For PostgreSQL integration

 - PostgreSQL: Target database for structured storage.

How to run this project:

# Clone repository
git clone https://github.com/TiagoJ7-byte/python-etl.git
cd python-etl

# Install dependencies
pip install -r requirements.txt

# Run ETL
python weather-api-to-postgresql-db.py
