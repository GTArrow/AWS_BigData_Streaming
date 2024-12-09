import json
import random
import time
from datetime import datetime
import math
# Log file path for Kinesis Agent
LOG_FILE_PATH = '/tmp/aws-kinesis-agent.log'

# Pre-generated latitude and longitude points
LAT_LONG_POINTS = [
    (round(random.uniform(45.53, 46), 10),
     round(random.uniform(-77.5, -79), 10))
    for _ in range(100)
]

# Generate sine wave parameters
sleep_time = 300 # sleep 300 sec; (5 minutes)
time_period = 60  # A full sine wave cycle over 60 iterations
amplitude_temp = 5  # Temperature fluctuation ±5
amplitude_humid = 10 # Humidity fluctuation ±10
base_temp = 60  # Base temperature
base_humidity = 30  # Base humidity

def calculate_ffwi(temperature, humidity, wind_speed):
    """
    Calculate the Fosberg Fire Weather Index (FFWI).
    """
    try:
        F = 0
        # Calculate F (factor based on relative humidity and temperature)
        if humidity < 100:
            F = 1 - (2 * (100 - humidity) / ((100 - humidity) + temperature))
            F = max(F, 0)
        else:
            F = 0  # If humidity is 100%, set F to 0, as there is no fire risk.

        # Calculate FFWI based on the formula
        ffwi = ((1 + wind_speed) / 0.3002) * math.sqrt(F)
        ffwi = max(0, min(ffwi, 100))  # Ensure FFWI is in range [0, 100]
        return ffwi
    except Exception as e:
        print(f"Error calculating FFWI: {e}")
        return None

def generate_mock_data(index):
    """
    Generate mock weather data.
    """

    timestamp = datetime.utcnow().isoformat()
    latitude, longitude = LAT_LONG_POINTS[index % len(LAT_LONG_POINTS)]
    temp_variation = amplitude_temp * math.sin(2 * math.pi * (index / time_period))
    humid_variation = amplitude_humid * math.sin(2 * math.pi * (index / time_period))

    temperature = round(base_temp + temp_variation, 2)
    humidity = round(base_humidity + humid_variation, 2)
    wind_speed = round(random.uniform(1, 30), 2)

    if(index % 10 == 0):
        # Calculate FFWI
        ffwi = calculate_ffwi(temperature, humidity, wind_speed)
        # Ensure FFWI > 0
        if ffwi == 0:
            temperature = random.uniform(70, 80)  # increase temperature to increase fire risk
            ffwi = calculate_ffwi(temperature, humidity, wind_speed)

    mock_data = {
        'timestamp': timestamp,
        'latitude': latitude,
        'longitude': longitude,
        'temperature': temperature,
        'humidity': humidity,
        'windSpeed': wind_speed
    }
    return mock_data

def write_data_to_file():
    # Continuously write mock data to the log file every 5 seconds
    index = 0 
    while True:
        try:
            with open(LOG_FILE_PATH, 'a') as log_file:
                records = []
                for i in range(500):
                    coordinate = LAT_LONG_POINTS[i % len(LAT_LONG_POINTS)]
                    mock_data = generate_mock_data(index)
                    mock_data['latitude'] = coordinate[0]
                    mock_data['longitude'] = coordinate[1]
                    records.append({
                        'Data': json.dumps(mock_data),
                        'PartitionKey': str(random.randint(1, 100))  # Random Partition Key
                    })
                
                index+=1
                for record in records:
                    log_file.write(json.dumps(record) + '\n')
                print(f"Mock data written to file: {records}")
            # Wait for 300 seconds before writing the next record
            time.sleep(300)
        except Exception as e:
            print(f"Error writing data to file: {e}")

if __name__ == "__main__":
    print("Generating and writing mock data to file...")
    write_data_to_file()
