import json
import random
import time
from datetime import datetime
import boto3
import math

# Kinesis Stream Details
STREAM_NAME = "weather_data_stream"  # Replace with your stream name
AWS_REGION = "us-east-2"             # Replace with your AWS region
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

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
    wind_speed = round(random.uniform(20, 30), 2)

    #ffwi = calculate_ffwi(temperature, humidity, wind_speed)
    #if(index % 10 == 0):
    #    # Calculate FFWI
    #    ffwi = calculate_ffwi(temperature, humidity, wind_speed)
    #    # Ensure FFWI > 0
    #    if ffwi == 0:
    #        temperature = random.uniform(70, 80)  # increase temperature to increase fire risk
    #        ffwi = calculate_ffwi(temperature, humidity, wind_speed)

    mock_data = {
        'timestamp': timestamp,
        'latitude': latitude,
        'longitude': longitude,
        'temperature': temperature,
        'humidity': humidity,
        'windSpeed': wind_speed
    }
    return mock_data

def send_data_to_kinesis():
    """
    Continuously send mock data to Kinesis Data Stream.
    """
    index = 0 
    while True:
        try:
            # Generate 100 records
            records = []
            for i in range(100):
                coordinate = LAT_LONG_POINTS[i % len(LAT_LONG_POINTS)]
                mock_data = generate_mock_data(index)
                mock_data['latitude'] = coordinate[0]
                mock_data['longitude'] = coordinate[1]
                records.append({
                    'Data': json.dumps(mock_data),
                    'PartitionKey': str(random.randint(1, 100))  # Random Partition Key
                })

            # Send the batch of records to Kinesis
            response = kinesis_client.put_records(StreamName=STREAM_NAME, Records=records)
            
            # Log the response
            print(f"Kinesis response: {response}")
            
            # Wait before sending the next record
            time.sleep(sleep_time)
            index+=1
        
        except Exception as e:
            print(f"Error sending data to Kinesis: {e}")
            time.sleep(sleep_time)

if __name__ == "__main__":
    print("Generating and sending mock data to Kinesis Data Stream...")
    send_data_to_kinesis()
