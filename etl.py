import requests
import pandas as pd
import psycopg2
from db_config import DB_CONFIG

# Replace with your actual OpenWeather API key
API_KEY = "1947b19a1347b7a7dfac8803808b6f17"  
CITY = "London"  # You can change this to any city
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def extract_weather():
    """Get current weather data from OpenWeather API."""
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        return {
            "city": CITY,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"]
        }
    else:
        raise Exception("Failed to fetch data from OpenWeather API")

def load_to_db(data):
    """Insert weather data into PostgreSQL table."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT,
            humidity INT,
            weather VARCHAR(100),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Insert data
    cursor.execute('''
        INSERT INTO weather_data (city, temperature, humidity, weather)
        VALUES (%s, %s, %s, %s);
    ''', (data["city"], data["temperature"], data["humidity"], data["weather"]))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    weather_data = extract_weather()
    print("Weather data:", weather_data)
    load_to_db(weather_data)
    print("✅ Weather data loaded into database.")

if __name__ == "__main__":
    main()
