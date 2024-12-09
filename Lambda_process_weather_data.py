import base64
import json
import math


def calculate_ffwi(temperature, humidity, wind_speed):
    """
    Calculate the Fosberg Fire Weather Index (FFWI).
    
    Parameters:
    temperature (float): Temperature in Fahrenheit (°F).
    humidity (float): Relative Humidity in percentage (%).
    wind_speed (float): Wind speed in miles per hour (mph).
    
    Returns:
    float: Calculated FFWI value.
    """
    # Calculate F (factor based on relative humidity and temperature)
    if humidity < 100:
        F = 1 - (2 * (100 - humidity) / ((100 - humidity) + temperature))
        F = max(F, 0) 
    else:
        F = 0  # If humidity is 100%, set F to 0, as there is no fire risk.

    # Calculate FFWI based on the formula
    ffwi = ((1 + wind_speed) / 0.3002) * math.sqrt(F)
    
    # Ensure FFWI is within the standard range [0, 100]
    ffwi = max(0, min(ffwi, 100))
    
    return ffwi


def lambda_handler(event, context):
    output = []
    
    for record in event['records']:
        # Decode the data

        payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))
        
        # Extract weather data
        temperature = payload.get('temperature')
        humidity = payload.get('humidity')
        windSpeed = payload.get('windSpeed')

        print(f"temperature: {temperature}, humidity: {humidity}, windSpeed: {windSpeed}")

        # Calculate FFWI
        ffwi = calculate_ffwi(temperature, humidity, windSpeed)
        payload['ffwi'] = ffwi  # Add FFWI to the record
        
        # Encode the record back for Firehose
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(payload).encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)
    
    return {'records': output}

