import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


# SUNRISE/SUNSET
# set up database and tables
def setup_database():
    conn = sqlite3.connect('sunrise_sunset.db')
    cur = conn.cursor()

    # create tables
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE
                )
                ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS SunriseSunset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_id INTEGER,
                sunrise TEXT,
                sunset TEXT,
                FOREIGN KEY (date_id) REFERENCES Dates(id)
                )
                ''')
    conn.commit()
    conn.close()

# get sunrise and sunset from API
def get_sunrise_sunset(latitude, longitude, date):
    try:
        url = f"https://api.sunrisesunset.io/json?lat={latitude}&lng={longitude}&date={date}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK':
                return data['results']
            else:
                return None
        else:   
            return None
    except:
        return None

# get and store sunrise and sunset data
def get_and_store_data():
    conn = sqlite3.connect('sunrise_sunset.db')
    cur = conn.cursor()

    cur.execute('SELECT date FROM Dates ORDER BY date DESC LIMIT 1')
    last_date_row = cur.fetchone()

    if last_date_row:
        last_date = datetime.strptime(last_date_row[0], '%Y-%m-%d')
    else:
        # if not time data, start from today
        last_date = datetime.today() - timedelta(days = 1)

    # keep track of items added in this run
    items_added = 0
    
    # store data for 25 days
    for days in range(25):
        next_date = last_date + timedelta(days=1)
        date_str = next_date.strftime('%Y-%m-%d')

        # Check if the date already exists in the Dates table
        cur.execute('SELECT id FROM Dates WHERE date = ?', (date_str,))
        if cur.fetchone() is None:
            # Get data from API
            result = get_sunrise_sunset(42.2808, -83.7430, date_str)
            if result:
                sunrise = result.get('sunrise')
                sunset = result.get('sunset')

                # insert date into Date table
                cur.execute('INSERT INTO Dates (date) VALUES (?)', (date_str,))
                date_id = cur.lastrowid

                # insert sunrise and sunset times into SunriseSunset table
                cur.execute('''INSERT INTO SunriseSunset (date_id, sunrise, sunset) 
                           VALUES (?, ?, ?)''', (date_id, sunrise, sunset))
            
                # increment the count of items added
                items_added += 1
                conn.commit()
            else:
                None
        else:
            None
        last_date = next_date
    conn.close()

# calculate data
def process_and_calculate_data():
    conn = sqlite3.connect('sunrise_sunset.db')
    cur = conn.cursor()

    # join Dates and SunriseSunset tables
    cur.execute('''
    SELECT d.date, s.sunrise, s.sunset
    FROM Dates d
    JOIN SunriseSunset s ON d.id = s.date_id
                ''')
    
    rows = cur.fetchall()

    # calculations
    day_counts = [0] * 7
    sunrise_times = []
    sunset_times = []
    dates = []

    for row in rows:
        date_str, sunrise_str, sunset_str = row
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        # Count each day of the week (Sunday=0, Monday=1, ..., Saturday=6)
        day_of_week = date_obj.weekday()
        day_counts[day_of_week] += 1

        sunrise_time = datetime.strptime(sunrise_str, '%I:%M:%S %p').time()
        sunset_time = datetime.strptime(sunset_str, '%I:%M:%S %p').time()

        dates.append(date_obj)
        sunrise_times.append(sunrise_time)
        sunset_times.append(sunset_time)
        
    # average sunrise and sunset times
    avg_sunrise_time = average_time(sunrise_times)
    avg_sunset_time = average_time(sunset_times)

    # write results to a file
    with open('calculated_data.txt', 'w') as file:
        file.write('Day of the week counts (Sunday=0, ..., Saturday=6):\n')
        file.write(', '.join(f'{day}: {count}' for day, count in enumerate(day_counts)) + '\n')
        file.write(f'Average sunrise time: {avg_sunrise_time}\n')
        file.write(f'Average sunset time: {avg_sunset_time}\n')

    conn.close()
    return day_counts, sunrise_times, sunset_times, dates

# how to caluclate average time for function above: process_and_calculate_data
def average_time(times):
    total_seconds = sum(t.hour * 3600 + t.minute * 60 + t.second for t in times)
    avg_seconds = total_seconds // len(times)
    return f'{avg_seconds // 3600:02}:{(avg_seconds % 3600) // 60:02}:{avg_seconds % 60:02}'

# Calculate difference between sunset and sunrise times
def calculate_difference(sunrise_times, sunset_times):
    differences = []
    for sunrise_time, sunset_time in zip(sunrise_times, sunset_times):
        sunrise_hour = time_to_hours(sunrise_time)
        sunset_hour = time_to_hours(sunset_time)
        difference = sunset_hour - sunrise_hour
        differences.append(difference)
    return differences

# convert times to hours for plotting
def time_to_hours(t):
    return t.hour + t.minute / 60 + t.second / 3600

# VISUALS FOR DATA
def visualize_data(day_counts, sunrise_times, sunset_times, dates):

    # Line Plot: Sunrise and Sunset Times
    dates_sorted = pd.date_range(start='2024-08-01', end='2024-12-01', periods=len(sunrise_times))

    avg_sunrise_seconds = [(t.hour * 3600 + t.minute * 60 + t.second) / 3600 for t in sunrise_times]
    avg_sunset_seconds = [(t.hour * 3600 + t.minute * 60 + t.second) / 3600 for t in sunset_times]

    # Calculate differences between sunset and sunrise times
    differences = [sunset - sunrise for sunrise, sunset in zip(avg_sunrise_seconds, avg_sunset_seconds)]

    plt.figure(figsize=(10, 6))
    plt.plot(dates_sorted, avg_sunrise_seconds, label='Sunrise Times', color='orange')
    plt.plot(dates_sorted, avg_sunset_seconds, label='Sunset Times', color='red')
    plt.plot(dates_sorted, differences, label='Hours of Daylight', color='green') # Difference (Sunset - Sunrise)
    plt.xlabel('Date')
    plt.ylabel('Time (Hour of the Day)')
    plt.title('Sunrise and Sunset Times Over Time')
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    setup_database()
    get_and_store_data()
    day_counts, sunrise_times, sunset_times, dates = process_and_calculate_data()
    visualize_data(day_counts, sunrise_times, sunset_times, dates)

if __name__ == "__main__":
    main()