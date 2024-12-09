import json
import boto3
import uuid
import time
import urllib.parse
import re

athena_client = boto3.client('athena')
quicksight_client = boto3.client('quicksight')  # Initialize QuickSight client

# Define the DataSourceArn variable
data_source_arn = 'arn:aws:quicksight:us-east-2:329599654349:datasource/96a72470-8dab-4633-9553-c80f44ac60de'  # Correct ARN for your Athena data source

def lambda_handler(event, context):
    # Extract the S3 bucket and file key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    print(f"Lambda triggered by file upload. Bucket: {bucket_name}, Object Key: {object_key}")
    
    # URL decode the object key
    decoded_object_key = urllib.parse.unquote(object_key)
    print(f"Decoded object key: {decoded_object_key}")
    
    # Use regex to extract year, month, and day from the object key
    try:
        # Regex pattern to extract year, month, and day
        pattern = r'year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})' # month, day 1 or 2 digits
        match = re.search(pattern, decoded_object_key)
        
        if match:
            year, month, day = match.groups()
            print(f"Extracted partition values - Year: {year}, Month: {month}, Day: {day}")
        else:
            raise ValueError(f"Unable to extract partition values from object key: {decoded_object_key}")
        
    except Exception as e:
        print(f"Error extracting partition values from object key {decoded_object_key}: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f'Error processing the file path: {str(e)}')
        }

    # Step 1: Repair the table partitions before running the query
    try:
        repair_query = f"MSCK REPAIR TABLE default.forest_weather_data_parquet;"
        print(f"Running MSCK REPAIR TABLE query: {repair_query}")
        
        # Start the Athena query to repair the table
        repair_response = athena_client.start_query_execution(
            QueryString=repair_query,
            QueryExecutionContext={'Database': 'default'},
            ResultConfiguration={'OutputLocation': f"s3://{bucket_name}/query-results/"}
        )
        repair_query_execution_id = repair_response['QueryExecutionId']
        print(f"Repair query started. QueryExecutionId: {repair_query_execution_id}")

        # Wait for the repair query to complete (you can also add a timeout here)
        athena_client.get_query_execution(QueryExecutionId=repair_query_execution_id)

    except Exception as e:
        print(f"Error repairing Athena table partitions: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error repairing table partitions: {str(e)}')
        }

    # Step 2: Define the Athena query (make sure to adjust the query based on your table and columns)
    query = f"""
    SELECT *
    FROM "default"."forest_weather_data_parquet"
    WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
    """
    
    print(f"Executing Athena query: {query}")
    
    # Set up Athena query execution parameters
    database = "default"  
    output_location = f"s3://{bucket_name}/query-results/"
    
    # Step 3: Start Athena query execution
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query started. QueryExecutionId: {query_execution_id}")
    except Exception as e:
        print(f"Error starting Athena query: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Athena query: {str(e)}')
        }
    
    # Check the query execution status
    status = "RUNNING"
    while status in ["RUNNING", "QUEUED"]:
        print(f"Checking query status... Current status: {status}")
        time.sleep(5)
        try:
            status_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
        except Exception as e:
            print(f"Error checking query status: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error checking query status: {str(e)}')
            }
        
    if status == "SUCCEEDED":
        print(f"Query succeeded. Results stored at: {output_location}")
        
        # Call the function to update QuickSight dataset after query success
        try:
            update_quicksight_dataset()
        except Exception as e:
            print(f"Error updating QuickSight dataset: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error updating QuickSight dataset: {str(e)}')
            }

    else:
        print(f"Query failed with status: {status}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Athena query executed successfully.')
    }

def update_quicksight_dataset():
    print("Updating QuickSight dataset...")
    aws_account_id = '329599654349'  
    dataset_id = '0d6c3e3d-fec8-46ad-a7ce-329359849637'  # Dataset ID in QuickSight

    table_key = 'forest-weather-data-parquet' 

    # Refresh the dataset using the update API
    try:
        response = quicksight_client.create_ingestion(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_id,
            IngestionId=str(uuid.uuid4())  # Unique ID for the ingestion process
        )
        print("QuickSight dataset updated successfully:", response)
    except Exception as e:
        print(f"Error updating QuickSight dataset: {e}")
