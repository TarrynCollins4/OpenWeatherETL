import requests
import psycopg2
import logging
import sys
from db_config import DB_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Get city from CLI or fallback to default
CITY = sys.argv[1] if len(sys.argv) > 1 else "Johannesburg"

# OpenWeather configuration
API_KEY = "1947b19a1347b7a7dfac8803808b6f17"  # Replace with your real API key
API_URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def fetch_weather():
    """
    Retrieves current weather data for the specified city from OpenWeather API.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        weather = response.json()
        return {
            "city": CITY,
            "temperature": weather["main"]["temp"],
            "humidity": weather["main"]["humidity"],
            "weather": weather["weather"][0]["description"]
        }
    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
        return None

def store_weather(data):
    """
    Saves weather data into the PostgreSQL database.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Ensure the weather_data table exists
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                        id SERIAL PRIMARY KEY,
                        city VARCHAR(50),
                        temperature FLOAT,
                        humidity INT,
                        weather VARCHAR(100),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Insert weather record
                cursor.execute("""
                    INSERT INTO weather_data (city, temperature, humidity, weather)
                    VALUES (%s, %s, %s, %s);
                """, (data["city"], data["temperature"], data["humidity"], data["weather"]))

        logging.info(f"Weather data for {data['city']} saved successfully.")
    except Exception as e:
        logging.error(f"Database insert failed: {e}")

def main():
    logging.info(f"Starting weather ETL for city: {CITY}")
    weather_data = fetch_weather()
    if weather_data:
        store_weather(weather_data)
    else:
        logging.warning("No data to store.")

if __name__ == "__main__":
    main()
