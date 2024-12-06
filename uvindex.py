import requests
import sqlite3
from datetime import datetime, timedelta

UV_API_KEY = 'openuv-9uguimrm3yo9xra-io'  # Replace with your OpenUV API Key
UV_BASE_URL = 'https://api.openuv.io/api/v1/uv'

# Function to create the necessary tables in the SQLite database
def create_tables():
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    # Create the locations table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        latitude REAL,
        longitude REAL,
        altitude REAL
    )''')

    # Create the uv_data table
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


# Function to get UV index data for a specific date
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
            return {
                'date': date,
                'max_uv': uv_max,
                'min_uv': uv,
                'max_uv_time': uv_max_time
            }
    return None


# Function to collect UV data for a given month
def collect_uv_month_data(latitude, longitude, altitude, start_date, end_date, collected_rows=0):
    uv_data = []
    current_date = start_date

    # Check for existing data in the database for this location and date range
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    while current_date <= end_date and len(uv_data) < 25:
        cursor.execute('''SELECT 1 FROM uv_data WHERE date = ? AND location_id IN
                          (SELECT id FROM locations WHERE latitude = ? AND longitude = ? AND altitude = ?)''',
                       (current_date, latitude, longitude, altitude))
        existing_data = cursor.fetchone()

        if not existing_data:  # No data exists for this date, so we can fetch
            print(f"Attempting to fetch data for {current_date}")
            uv = get_uv_index_for_date(latitude, longitude, altitude, current_date)
            if uv:
                uv_data.append(uv)
                print(f"Collected data for {current_date}: {uv}")
        else:
            print(f"Data already exists for {current_date}, skipping.")

        # Increment date
        current_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    conn.close()
    print(f"Collected {len(uv_data)} rows of UV data.")
    return uv_data  # Return the correct variable name: uv_data



# Function to store UV data into the database
def store_uv_data_with_min_max(uv_data, latitude, longitude, altitude):
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    # Insert or ignore location data
    cursor.execute('''INSERT OR IGNORE INTO locations (latitude, longitude, altitude)
                      VALUES (?, ?, ?)''', (latitude, longitude, altitude))

    # Get the location_id of the inserted location
    cursor.execute('''SELECT id FROM locations WHERE latitude = ? AND longitude = ? AND altitude = ?''',
                   (latitude, longitude, altitude))
    location_id = cursor.fetchone()[0]

    # Insert UV data for the location
    for entry in uv_data:
        cursor.execute('''INSERT INTO uv_data (location_id, date, max_uv, min_uv, max_uv_time)
                          VALUES (?, ?, ?, ?, ?)''', 
                       (location_id, entry['date'], entry['max_uv'], entry['min_uv'], entry['max_uv_time']))
        print(f"Inserted data: {entry}")

    conn.commit()
    conn.close()


# Function to calculate the average UV index from the stored data
def calculate_average_uv():
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    cursor.execute('SELECT max_uv FROM uv_data')
    rows = cursor.fetchall()

    conn.close()

    if rows:
        total_uv = sum([row[0] for row in rows])
        return total_uv / len(rows)
    else:
        return 0


# Main function that ties everything together
def main():
    # Define location coordinates and date range
    latitude = 42.2808  # Example: University of Michigan
    longitude = -83.7430
    altitude = 200  # Example altitude in meters
    start_date = '2024-08-01'
    end_date = '2024-12-01'

    # Create the database tables
    create_tables()

    # Collect and store UV data
    uv_data = collect_uv_month_data(latitude, longitude, altitude, start_date, end_date)
    if uv_data:
        store_uv_data_with_min_max(uv_data, latitude, longitude, altitude)

    # Calculate and print the average UV index
    average_uv = calculate_average_uv()
    print(f"Average UV index: {average_uv:.2f}")


if __name__ == "__main__":
    main()


       
