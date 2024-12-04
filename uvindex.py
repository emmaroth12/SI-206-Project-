import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px


UV_API_KEY = 'openuv-9uguimrm3yo9xra-io'  # Replace with your actual API key
UV_BASE_URL = 'https://api.openuv.io/api/v1/uv'

def get_uv_index_for_date(latitude, longitude, altitude, date):
    headers = {'x-access-token': UV_API_KEY}
    dt = date  # Date in 'YYYY-MM-DD' format
    params = {
        'lat': latitude,
        'lng': longitude,
        'alt': altitude,
        'dt': dt
    }

    response = requests.get(UV_BASE_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Check if the UV data is available
        if 'result' in data and 'uv' in data['result'] and 'uv_max' in data['result']:
            uv = data['result']['uv']
            uv_max = data['result']['uv_max']
            uv_max_time = data['result']['uv_max_time']
            
            print(f"Data for {dt}: Max UV: {uv_max}, Min UV: {uv}, Max UV Time: {uv_max_time}")  # Debug print
            
            return {
                'date': dt,
                'max_uv': uv_max,  # Max UV index for the given date
                'min_uv': uv,      # Current UV index, which is the minimum for the timestamp
                'max_uv_time': uv_max_time  # Time when the max UV occurred
            }
        else:
            print(f"No UV data available for {dt}")
            return None
    else:
        print(f"Error fetching UV data for {date}: {response.status_code}")
        return None

# Function to collect UV data for an entire date range
def collect_uv_month_data(latitude, longitude, altitude, start_date, end_date):
    uv_data = []
    current_date = start_date
    
    while current_date <= end_date:
        uv = get_uv_index_for_date(latitude, longitude, altitude, current_date)
        
        if uv:
            uv_data.append(uv)
        
        current_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Collected {len(uv_data)} days of UV data.")  # Debug print
    return uv_data

# Function to create the database and add columns for max and min UV
def create_db():
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    # Create the table with columns for max UV, min UV, and max UV time
    cursor.execute('''CREATE TABLE IF NOT EXISTS uv_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        max_uv REAL,
                        min_uv REAL,
                        max_uv_time TEXT)''')
    
    # Ensure the 'max_uv_time' column exists (add it if it does not exist)
    try:
        cursor.execute('''ALTER TABLE uv_data ADD COLUMN max_uv_time TEXT''')
    except sqlite3.OperationalError:
        # If the column already exists, skip it
        pass
    
    conn.commit()
    conn.close()
    print("Database schema is set up correctly.")  # Debug print

# Function to store collected UV data into the database
def store_uv_data_with_min_max(uv_data):
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()
    
    # Insert data for each date with both max and min UV values
    for entry in uv_data:
        cursor.execute('''INSERT INTO uv_data (date, max_uv, min_uv, max_uv_time) 
                          VALUES (?, ?, ?, ?)''', 
                          (entry['date'], entry['max_uv'], entry['min_uv'], entry['max_uv_time']))
    
    conn.commit()
    conn.close()
    print("Data stored in database.")  # Debug print

# Function to fetch UV data from the database for a specific date range
def fetch_uv_data_in_range(start_date, end_date):
    conn = sqlite3.connect('uv_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT date, max_uv, min_uv, max_uv_time FROM uv_data WHERE date BETWEEN ? AND ? ORDER BY date ASC", (start_date, end_date))
    rows = cursor.fetchall()

    conn.close()

    if rows:
        return [{'date': row[0], 'max_uv': row[1], 'min_uv': row[2], 'max_uv_time': row[3]} for row in rows]
    else:
        print(f"No data found in the database for {start_date} to {end_date}.")  # Debug print
        return []
    
def plot_uv_data(uv_data):
    # Convert the UV data to a DataFrame for easy plotting
    df = pd.DataFrame(uv_data)
    
    # Convert date string to datetime for better handling
    df['date'] = pd.to_datetime(df['date'])
    
    # Create a line plot for max and min UV
    fig = px.line(df, x='date', y=['max_uv', 'min_uv'], title='UV Index over Time', labels={'date': 'Date', 'value': 'UV Index'})
    
    # Show the plot
    fig.show()

# Main function
def main():
    latitude = 42.2808  # Latitude for the location
    longitude = -83.7430  # Longitude for the location
    altitude = 200  # Altitude for the location (optional)
    start_date = '2024-08-01'  # Start date for the range
    end_date = '2024-11-30'  # End date for the range

    # Create the database and tables if they don't exist
    create_db()  
    
    # Collect UV data for the given range
    uv_data = collect_uv_month_data(latitude, longitude, altitude, start_date, end_date)
    
    if uv_data:
        # Store the collected UV data with max and min values
        store_uv_data_with_min_max(uv_data)
    
    # Fetch the stored data from the database
    fetched_data = fetch_uv_data_in_range(start_date, end_date)
    
    if fetched_data:
        print(f"Fetched {len(fetched_data)} records from the database.")  # Debug print
        # Plot the fetched data
        plot_uv_data(fetched_data)
    else:
        print("No data found to fetch.")

# Run the program
if __name__ == "__main__":
    main()