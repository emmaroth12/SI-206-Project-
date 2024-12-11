import requests
import sqlite3
from collections import defaultdict

# API Configuration
WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/era5"
WEATHER_PARAMS = {
    "latitude": 42.28,
    "longitude": -83.74,
    "start_date": "2024-08-01",
    "end_date": "2024-11-30",
    "hourly": "temperature_2m"
}

# Fetch weather data with a limit of 25 items
def fetch_weather_data(api_url, params, start_date):
    params["start_date"] = start_date
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        times = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]

        daily_temperatures = defaultdict(list)
        for time, temp in zip(times, temperatures):
            date = time.split("T")[0]  # Extract date
            daily_temperatures[date].append(temp)

        # Convert to a structured list
        result = [
            {"date": date, "avg_temp": sum(temps) / len(temps)}
            for date, temps in daily_temperatures.items()
        ]

        return result[:25]  # Limit to 25 rows
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

# Setup the database
def setup_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            avg_temp REAL NOT NULL
        )
    ''')
    return conn, cursor

# Insert weather data into the database
def insert_weather_data(cursor, weather_data):
    for record in weather_data:
        cursor.execute('''
            INSERT OR IGNORE INTO weather (date, avg_temp)
            VALUES (?, ?)
        ''', (record["date"], record["avg_temp"]))

# Get the last inserted date from the database
def get_last_inserted_date(cursor):
    cursor.execute('SELECT MAX(date) FROM weather')
    result = cursor.fetchone()
    return result[0] if result[0] else "2024-08-01"  # Default to API start_date

# Calculate the overall average temperature
def calculate_overall_average(cursor):
    cursor.execute('SELECT AVG(avg_temp) FROM weather')
    result = cursor.fetchone()
    if result and result[0] is not None:
        return result[0]
    return None

def fetch_daily_average_temperatures(db_name):
    """
    Fetches daily average temperatures from the weather table in the database.
    
    Parameters:
    - db_name: str, name of the SQLite database.
    
    Returns:
    - DataFrame containing date and avg_temp columns.
    """
    conn = sqlite3.connect(db_name)
    query = """
    SELECT date, avg_temp 
    FROM weather 
    ORDER BY date;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


import matplotlib.pyplot as plt
import pandas as pd

def plot_data(db_name):
    """
    Fetches daily average temperatures from the database and plots them.
    
    Parameters:
    - db_name: str, name of the SQLite database.
    """
    # Fetch data from the database
    df = fetch_daily_average_temperatures(db_name)
    
    # Ensure the date column is in datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['avg_temp'], marker='o', linestyle='-', color='blue', linewidth=2)
    plt.title("Daily Average Temperatures", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Average Temperature (°C)", fontsize=12)
    plt.xticks(rotation=45, fontsize=10)
    plt.grid(alpha=0.4)
    plt.tight_layout()
    plt.show()

# Main Function
def main():
    db_name = 'weather.db'
    conn, cursor = setup_database(db_name)

    # Get the last inserted date and fetch new data
    last_date = get_last_inserted_date(cursor)
    weather_data = fetch_weather_data(WEATHER_API_URL, WEATHER_PARAMS, last_date)
    if weather_data:
        insert_weather_data(cursor, weather_data)
        conn.commit()
        print(f"Inserted {len(weather_data)} records into the database.")

    # Calculate and display the overall average temperature
    overall_avg = calculate_overall_average(cursor)
    if overall_avg is not None:
        print(f"The overall average temperature from August to November 30 is {overall_avg:.2f}°C.")
    else:
        print("No data available to calculate the average.")

    # Plot the daily average temperatures
    plot_data(db_name)

    conn.close()

if __name__ == "__main__":
    main()
