import pandas as pd
import time
import requests
import psycopg
import datetime
from datetime import datetime
from sqlalchemy import create_engine

# ----------------------
# Database Configuration
# ----------------------

DB_USER = 'postgres'
DB_PASSWORD = '123456'
DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'my_first_db'

# Table name to load data into
TABLE_NAME = 'time_average'

# SQLAlchemy connection string
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Connect using a context manager
with psycopg.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        result = cur.fetchone()
        print(result)


# ETL Functions
def get_data_from_api(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    return None

def average_temperature_per_minute(data):
    """
    Transforms API response to calculate average temperature per minute.
    Assumes 'temperature_2m' is a list of temperatures in Celsius,
    and 'time' is a list of corresponding timestamps in ISO 8601 format.
    """

    try:
        temperatures = data['hourly']['temperature_2m']
        times = data['hourly']['time']
        
        if not temperatures or not times or len(temperatures) != len(times):
            print("Data mismatch or missing.")
            return None

        # Create (minute, temperature) pairs
        from collections import defaultdict
        from datetime import datetime

        minute_buckets = defaultdict(list)

        for t_str, temp in zip(times, temperatures):
            dt = datetime.fromisoformat(t_str)
            minute_key = dt.strftime("%Y-%m-%d %H:%M")
            minute_buckets[minute_key].append(temp)

        # Calculate average per minute
        average_per_minute = {
            minute: sum(temps) / len(temps) for minute, temps in minute_buckets.items()
        }

        return average_per_minute

    except KeyError as e:
        print(f"Missing expected key in data: {e}")
        return None
    
def transform_data(average_temp):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return {
        'timestamp': now,
        'temperature': average_temp
    }

# Load to PostgreSQL
def load_to_postgres(record):
    print('record: ', record)
    df = pd.DataFrame(record)
    print('HELLO')
    print(df.head())
    df = df.groupby('minute').agg(
        temperature=('temperature', 'mean')
    ).reset_index()
    # Insert rows
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO average_temperatures (minute, avg_temperature_2m)
                VALUES (%s, %s)
                ON CONFLICT (minute) DO UPDATE
                SET avg_temperature_2m = EXCLUDED.avg_temperature_2m;
            """, (row['minute'], row['avg_temperature_2m']))
        conn.commit()

    conn.close()

    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print(f"Loaded: {record}")
    


if __name__ == "__main__":
    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    query_params = {"key": "value"}  # Adjust with real query parameters
        
    print("Starting API polling every 30 seconds...\n")
    while True:
        data = get_data_from_api(api_url, params=query_params)
        if data:
            print("New data received:")
            # Let's say this is the result from your average_temperature_per_minute(data)
            avg_dict = average_temperature_per_minute(data)

            # Convert to DataFrame properly
            df = pd.DataFrame(list(avg_dict.items()), columns=["minute", "avg_temperature_2m"])
            print(df)

            # Transform the data
            transformed = transform_data(df)
            print(transformed)
            
            # Persist the dataframe to the PostgreSQL database
            load_to_postgres(df)
        else:
            print("No data or error occurred.")

        print("Waiting 30 seconds...\n")
        time.sleep(20)