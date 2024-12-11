import requests
import re
import unittest
import sqlite3
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt



# Define API endpoint and parameters
base_url = "https://archive-api.open-meteo.com/v1/era5"

# Call the API
def fetch_weather_data(base_url, params):

    params = {
    "latitude": 42.28,  # Latitude for Ann Arbor, MI
    "longitude": -83.74,  # Longitude for Ann Arbor, MI
    "start_date": "2024-08-01",  # Start of the month
    "end_date": "2024-11-30",    # End of the month
    "hourly": "temperature_2m",  # Request hourly temperature at 2 meters above ground
}

    response = requests.get(base_url, params=params)

# Check if the response was successful
    if response.status_code == 200:

        # Parse the JSON response
        data = response.json()

        times = data["hourly"]["time"]  # List of timestamps
    
        temperatures = data["hourly"]["temperature_2m"]  # List of temperatures in Celsius

        # Organize temperatures by day
        daily_temperatures = []
        for time, temp in zip(times, temperatures):
            date = re.search(r'\d{4}-\d{2}-\d{2}(?=T)', time).group(0)  # Extract date

            daily_temperatures.append({"date": date, "temperature": temp})  
        return daily_temperatures
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []


#CREATE TABLE 
def setup_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create weather table
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            temperature REAL NOT NULL
        )
    ''')
    conn.commit()
    return conn, cursor

def insert_weather_data(weather_data):

    batch_size = 25
    total_records = len(weather_data)
    for i in range(0, total_records, batch_size):
        cursor.execute('''
            INSERT INTO weather (date, temperature)
            VALUES (?, ?)
        ''', (record["date"], record["temperature"]))

        initial=0
        cursor.execute('''
            SELECT MAX(id) FROM weather ''')
    fetch_data=cursor.fetchone()
    print(fetch_data)
    conn.commit()


def process_daily_temperatures_in_range(start_date, end_date):
    # Connect to the SQLite database
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()

    # Query to fetch daily minimum and maximum temperatures
    cursor.execute('''
        SELECT date, MIN(temperature) as min_temp, MAX(temperature) as max_temp
        FROM weather
        WHERE date BETWEEN ? AND ?
        GROUP BY date
    ''', (start_date, end_date))

    # Fetch all rows from the query result
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    # Return the result as a list of dictionaries
    if rows:
        return [{'date': row[0], 'min_temp': row[1], 'max_temp': row[2]} for row in rows]
    else:
        print(f"No data found in the database for {start_date} to {end_date}.")  # Debug print
        return []


import pandas as pd
import matplotlib.pyplot as plt

def plot_data(data):
    df = pd.DataFrame(data)

    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['min_temp'], label="Min Temperature", marker='o', linestyle='-', color='blue')
    plt.plot(df['date'], df['max_temp'], label="Max Temperature", marker='o', linestyle='-', color='red')

    # Customize the plot
    plt.title("Daily Min and Max Temperatures Over Time", fontsize=14)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Temperature (°C)", fontsize=12)

    # Show the plot
    plt.show()

def main():
    conn, cursor = setup_database('weather.db')
    weather = fetch_weather_data(base_url, [])
    insert_weather_data(weather)

if __name__ == "__main__":
    main()

    #daily_temperatures = process_daily_temperatures_in_range('2024-08-01', '2024-11-30')
    #for entry in daily_temperatures:
    #    print(f"Date: {entry['date']}, Min Temp: {entry['min_temp']}°C, Max Temp: {entry['max_temp']}°C")

    if daily_temperatures:
        plot_data(daily_temperatures)
        conn.close()


