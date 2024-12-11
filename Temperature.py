import requests
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd

# API Configuration
WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/era5"
WEATHER_PARAMS = {
    "latitude": 42.28,
    "longitude": -83.74,
    "hourly": "temperature_2m"
}
BATCH_SIZE = 25  # Maximum number of records to store per run

# Fetch weather data
def fetch_weather_data(api_url, params, start_date, end_date):
    params["start_date"] = start_date
    params["end_date"] = end_date
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        times = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]

        daily_temperatures = defaultdict(list)
        for time, temp in zip(times, temperatures):
            date = time.split("T")[0]  # Extract date
            daily_temperatures[date].append(temp)

        # Store raw temperature data as a list for each date
        result = [{"date": date, "temps": temps} for date, temps in daily_temperatures.items()]
        return result
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
            temps TEXT NOT NULL
        )
    ''')
    return conn, cursor

# Insert raw weather data into the database
def insert_weather_data(cursor, weather_data):
    for record in weather_data:
        # Convert the temperatures list to a comma-separated string for storage
        temps_str = ",".join(map(str, record["temps"]))
        cursor.execute('''
            INSERT OR IGNORE INTO weather (date, temps)
            VALUES (?, ?)
        ''', (record["date"], temps_str))

# Get the last inserted date from the database
def get_last_inserted_date(cursor):
    cursor.execute('SELECT MAX(date) FROM weather')
    result = cursor.fetchone()
    return result[0] if result[0] else None

# Fetch data to store in batches
def store_data_in_batches(db_name, api_url, params, batch_size):
    conn, cursor = setup_database(db_name)

    # Get the last inserted date
    last_date = get_last_inserted_date(cursor)

    # If no data exists in the database, set a default start date
    start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last_date else params.get("start_date")
    end_date = params.get("end_date")

    # Fetch and store data
    weather_data = fetch_weather_data(api_url, params, start_date, end_date)
    if weather_data:
        batch = weather_data[:batch_size]  # Limit the data to the batch size
        insert_weather_data(cursor, batch)
        conn.commit()
        print(f"Inserted {len(batch)} records into the database.")
    else:
        print("No new data fetched.")

    conn.close()

# Perform calculations on the data
def calculate_daily_averages(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT date, temps FROM weather')
    rows = cursor.fetchall()
    conn.close()

    # Calculate averages
    results = []
    for date, temps_str in rows:
        temps = list(map(float, temps_str.split(",")))  # Convert back to list of floats
        avg_temp = sum(temps) / len(temps)  # Calculate average
        results.append({"date": date, "avg_temp": avg_temp})

    return results

# Save calculations to file
def save_calculations_to_file(calculations, file_name="calculations.txt"):
    with open(file_name, "w") as file:
        file.write("Date, Average Temperature (°C)\n")
        for record in calculations:
            file.write(f"{record['date']}, {record['avg_temp']:.2f}\n")
    print(f"Calculations written to {file_name}.")

# Plot the data
def plot_data(calculations):
    df = pd.DataFrame(calculations)
    df["date"] = pd.to_datetime(df["date"])

    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["avg_temp"], marker="o", linestyle="-", linewidth=2)
    plt.title("Daily Average Temperatures", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Average Temperature (°C)", fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(alpha=0.4)
    plt.tight_layout()
    plt.show()

# Main function
def main_with_visualization():
    db_name = "weather.db"
    WEATHER_PARAMS["start_date"] = "2024-08-01"
    WEATHER_PARAMS["end_date"] = "2024-11-30"

    # Store data in batches
    store_data_in_batches(db_name, WEATHER_API_URL, WEATHER_PARAMS, BATCH_SIZE)

    # Calculate daily averages
    calculations = calculate_daily_averages(db_name)

    # Save calculations to file
    save_calculations_to_file(calculations)

    # Visualize the data
    plot_data(calculations)

if __name__ == "__main__":
    main_with_visualization()
