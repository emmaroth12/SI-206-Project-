# ------------------
# Merging Databases
# ------------------

import sqlite3

def merge_databases(db_filenames, merged_db_filename):
    """
    This function merges multiple SQLite databases into one.
    Args:
    - db_filenames: list of database filenames to merge.
    - merged_db_filename: filename of the merged database.
    """
    conn = sqlite3.connect(merged_db_filename)
    cur = conn.cursor()

    # Create the combined tables
    cur.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        temps TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS uv_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date TEXT,
        max_uv REAL,
        min_uv REAL,
        max_uv_time TEXT
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS SunriseSunset (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_id INTEGER,
        sunrise TEXT,
        sunset TEXT,
        FOREIGN KEY (date_id) REFERENCES Dates(id)
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS Dates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE
    )''')

    for db_filename in db_filenames:
        attach_db_name = db_filename.split('.')[0]
        cur.execute(f"ATTACH DATABASE '{db_filename}' AS {attach_db_name}")
        
        # Check and insert data from weather table
        tables = cur.execute(f"SELECT name FROM {attach_db_name}.sqlite_master WHERE type='table'").fetchall()
        table_names = {table[0] for table in tables}

        if 'weather' in table_names:
            cur.execute(f"INSERT OR IGNORE INTO weather (date, temps) SELECT date, temps FROM {attach_db_name}.weather")

        # Check and insert data from uv_data table
        if 'uv_data' in table_names:
            cur.execute(f"INSERT OR IGNORE INTO uv_data (location_id, date, max_uv, min_uv, max_uv_time) SELECT location_id, date, max_uv, min_uv, max_uv_time FROM {attach_db_name}.uv_data")

        # Check and insert data from SunriseSunset and Dates tables and update foreign keys
        if 'Dates' in table_names:
            cur.execute(f"INSERT OR IGNORE INTO Dates (date) SELECT date FROM {attach_db_name}.Dates")
        
        if 'SunriseSunset' in table_names:
            cur.execute(f"INSERT OR IGNORE INTO SunriseSunset (date_id, sunrise, sunset) SELECT date_id, sunrise, sunset FROM {attach_db_name}.SunriseSunset")

    conn.commit()
    conn.close()


# -----------------------
# Weather Data Functions
# -----------------------

import requests
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd

WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/era5"
WEATHER_PARAMS = {
    "latitude": 42.28,
    "longitude": -83.74,
    "hourly": "temperature_2m"
}
BATCH_SIZE = 25

def fetch_weather_data(api_url, params, start_date, end_date):
    params["start_date"] = start_date 
    params["end_date"] = end_date
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        times = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]

        daily_temperatures = defaultdict(list)  # create dictionary
        for time, temp in zip(times, temperatures):
            date = time.split("T")[0]  # Extract date
            daily_temperatures[date].append(temp)

        result = [{"date": date, "temps": temps} for date, temps in daily_temperatures.items()]
        return result
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

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

def insert_weather_data(cursor, weather_data):
    for record in weather_data:
        temps_str = ",".join(map(str, record["temps"]))
        cursor.execute('''
            INSERT OR IGNORE INTO weather (date, temps)
            VALUES (?, ?)
        ''', (record["date"], temps_str))

def get_last_inserted_date(cursor):
    cursor.execute('SELECT MAX(date) FROM weather')
    result = cursor.fetchone()
    return result[0] if result[0] else None

def store_data_in_batches(db_name, api_url, params, batch_size):
    conn, cursor = setup_database(db_name)

    last_date = get_last_inserted_date(cursor)
    start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last_date else params.get("start_date")
    end_date = params.get("end_date")

    weather_data = fetch_weather_data(api_url, params, start_date, end_date)
    if weather_data:
        batch = weather_data[:batch_size]
        insert_weather_data(cursor, batch)
        conn.commit()
        print(f"Inserted {len(batch)} records into the database.")
    conn.close()

def calculate_daily_averages(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT date, temps FROM weather')
    rows = cursor.fetchall()
    conn.close()

    results = []
    for date, temps_str in rows:
        temps = list(map(float, temps_str.split(",")))
        avg_temp = sum(temps) / len(temps)
        results.append({"date": date, "avg_temp": avg_temp})

    return results

def save_calculations_to_file(calculations, file_name="calculations.txt"):
    with open(file_name, "w") as file:
        file.write("Date, Average Temperature (°C)\n")
        for record in calculations:
            file.write(f"{record['date']}, {record['avg_temp']:.2f}\n")
    print(f"Calculations written to {file_name}.")

def plot_data(calculations):
    df = pd.DataFrame(calculations)
    df["date"] = pd.to_datetime(df["date"])

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["avg_temp"], marker="o", linestyle="-", linewidth=2)
    plt.title("Daily Average Temperatures", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Average Temperature (°C)", fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(alpha=0.4)
    plt.tight_layout()
    plt.show()

def main_with_visualization():
    db_name = "merged_data.db"
    WEATHER_PARAMS["start_date"] = "2024-08-01"
    WEATHER_PARAMS["end_date"] = "2024-11-30"

    store_data_in_batches(db_name, WEATHER_API_URL, WEATHER_PARAMS, BATCH_SIZE)
    calculations = calculate_daily_averages(db_name)
    save_calculations_to_file(calculations)
    plot_data(calculations)

if __name__ == "__main__":
    main_with_visualization()

# ------------------
# UV Data Functions
# ------------------

import requests
import sqlite3
from datetime import datetime, timedelta

UV_API_KEY = 'openuv-9uguimrm3yo9xra-io'
UV_BASE_URL = 'https://api.openuv.io/api/v1/uv'

def create_tables():
    conn = sqlite3.connect('merged_data.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        latitude REAL,
        longitude REAL,
        altitude REAL
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS uv_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date TEXT,
        max_uv REAL,
        min_uv REAL,
        max_uv_time TEXT,
        FOREIGN KEY (location_id) REFERENCES locations (id)
    )''')

    conn.commit()
    conn.close()

def get_uv_index_for_date(latitude, longitude, altitude, date):
    headers = {'x-access-token': UV_API_KEY}
    params = {'lat': latitude, 'lng': longitude, 'alt': altitude, 'dt': date}

    response = requests.get(UV_BASE_URL, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'result' in data and 'uv' in data['result'] and 'uv_max' in data['result']:
            uv = data['result']['uv']
            uv_max = data['result']['uv_max']
            uv_max_time = data['result']['uv_max_time']
            return {'date': date, 'max_uv': uv_max, 'min_uv': uv, 'max_uv_time': uv_max_time}
    return None

def collect_uv_month_data(latitude, longitude, altitude, start_date, end_date, collected_rows=0):
    uv_data = []
    current_date = start_date

    conn = sqlite3.connect('merged_data.db')
    cursor = conn.cursor()

    while current_date <= end_date and len(uv_data) < 25:
        cursor.execute('''SELECT 1 FROM uv_data WHERE date = ? AND location_id IN
                          (SELECT id FROM locations WHERE latitude = ? AND longitude = ? AND altitude = ?)''',
                       (current_date, latitude, longitude, altitude))
        existing_data = cursor.fetchone()

        if not existing_data:  
            print(f"Attempting to fetch data for {current_date}")
            uv = get_uv_index_for_date(latitude, longitude, altitude, current_date)
            if uv:
                uv_data.append(uv)
                print(f"Collected data for {current_date}: {uv}")
        else:
            print(f"Data already exists for {current_date}, skipping.")

        current_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    conn.close()
    print(f"Collected {len(uv_data)} rows of UV data.")
    return uv_data

def store_uv_data_with_min_max(uv_data, latitude, longitude, altitude):
    conn = sqlite3.connect('merged_data.db')
    cursor = conn.cursor()

    cursor.execute('''INSERT OR IGNORE INTO locations (latitude, longitude, altitude)
                      VALUES (?, ?, ?)''', (latitude, longitude, altitude))

    cursor.execute('''SELECT id FROM locations WHERE latitude = ? AND longitude = ? AND altitude = ?''',
                   (latitude, longitude, altitude))
    location_id = cursor.fetchone()[0]

    for entry in uv_data:
        cursor.execute('''INSERT INTO uv_data (location_id, date, max_uv, min_uv, max_uv_time)
                          VALUES (?, ?, ?, ?, ?)''', 
                       (location_id, entry['date'], entry['max_uv'], entry['min_uv'], entry['max_uv_time']))
        print(f"Inserted data: {entry}")

    conn.commit()
    conn.close()

def calculate_average_uv():
    conn = sqlite3.connect('merged_data.db')
    cursor = conn.cursor()

    cursor.execute('SELECT max_uv FROM uv_data')
    rows = cursor.fetchall()

    conn.close()

    if rows:
        total_uv = sum([row[0] for row in rows])
        return total_uv / len(rows)
    else:
        return 0

def main():
    latitude = 42.2808
    longitude = -83.7430
    altitude = 200
    start_date = '2024-08-01'
    end_date = '2024-12-01'

    create_tables()
    uv_data = collect_uv_month_data(latitude, longitude, altitude, start_date, end_date)
    if uv_data:
        store_uv_data_with_min_max(uv_data, latitude, longitude, altitude)

    average_uv = calculate_average_uv()
    print(f"Average UV index: {average_uv:.2f}")

if __name__ == "__main__":
    main()

# -----------------------------
# Sunrise/Sunset Data Functions
# -----------------------------

import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

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

def main_combined():
    # Step 1: Define the filenames of the individual databases
    db_files = ['weather.db', 'uv_data.db', 'sunrise_sunset.db']
    
    # Step 2: Name of the merged database
    merged_db = 'merged_data.db'
    
    # Step 3: Merge the databases into one
    merge_databases(db_files, merged_db)
    
    # Step 4: Weather data operations
    WEATHER_PARAMS["start_date"] = "2024-08-01"
    WEATHER_PARAMS["end_date"] = "2024-11-30"
    store_data_in_batches(merged_db, WEATHER_API_URL, WEATHER_PARAMS, BATCH_SIZE)
    weather_calculations = calculate_daily_averages(merged_db)
    save_calculations_to_file(weather_calculations, "weather_calculations.txt")
    plot_data(weather_calculations)

    # Step 5: UV data operations
    latitude = 42.2808
    longitude = -83.7430
    altitude = 200
    start_date = '2024-08-01'
    end_date = '2024-12-01'
    
    create_tables()  # Ensure UV tables are created in the merged database
    uv_data = collect_uv_month_data(latitude, longitude, altitude, start_date, end_date)
    if uv_data:
        store_uv_data_with_min_max(uv_data, latitude, longitude, altitude)
    
    average_uv = calculate_average_uv()
    print(f"Average UV index: {average_uv:.2f}")

    # Step 6: Sunrise/Sunset operations
    setup_database(merged_db)  # Ensure SunriseSunset tables are created in the merged database
    get_and_store_data(merged_db)
    day_counts, sunrise_times, sunset_times, dates = process_and_calculate_data(merged_db)
    visualize_data(day_counts, sunrise_times, sunset_times, dates)

if __name__ == "__main__":
    main_combined()