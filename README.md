# AWS Cloud Big Data Streaming Solution

This repository contains scripts and configurations for simulating data ingestion, processing, and storage in an AWS-based big data streaming pipeline. Below is a description of each file and its role in the solution.

---

## **Files Overview**

### 1. **`api_simdata_gen.py`**
   - **Purpose**: Injects simulated IoT sensor data (coordinates, temperature, humidity, wind speed) into an AWS Kinesis Data Stream.
   - **Details**:
     - Uses the AWS Kinesis Data Stream API.
     - Simulates data from various geographical points.
     - Suitable for scenarios where programmatic control over data injection is needed.

---

### 2. **`kinesisagent_simdata_gen.py`**
   - **Purpose**: Injects simulated IoT sensor data into an AWS Kinesis Data Stream using the AWS Kinesis Data Agent.
   - **Details**:
     - Relies on the Kinesis Data Agent for file-based data ingestion.
     - Writes simulated data to log files that the agent monitors and streams to Kinesis.
     - Simpler alternative to the API-based approach but less customizable.

---

### 3. **`kinesis-spark-etl.py`**
   - **Purpose**: Specifies the Spark job for processing big data in AWS EMR (Elastic MapReduce) using MapReduce techniques.
   - **Details**:
     - Reads data from the Kinesis Data Stream.
     - Performs data transformation and aggregations (e.g., calculating the Fosberg Fire Weather Index).
     - Writes the processed results to Amazon S3, partitioned for efficient querying.

---

### 4. **Lambda Functions**

#### a. **`Lambda_process_weather_data.py`**
   - **Purpose**: An earlier version of the data processing pipeline using AWS Lambda.
   - **Details**:
     - Processes raw data from the Kinesis Data Stream.
     - Performs basic transformations and filtering.
     - Was later replaced by the Spark-based solution for handling larger-scale data processing.

#### b. **`Lambda_PushToAthena.py`**
   - **Purpose**: A consumer for the Kinesis Data Stream that backs up raw data to Amazon S3.
   - **Details**:
     - Serves as a backup mechanism for raw data ingestion.
     - Prepares the data for analysis with Amazon Athena by ensuring storage in an organized format in S3.

---

## **Solution Architecture**

1. **Data Ingestion**:
   - `api_simdata_gen.py` and `kinesisagent_simdata_gen.py` are responsible for injecting simulated sensor data into AWS Kinesis Data Streams.

2. **Data Processing**:
   - `kinesis-spark-etl.py` handles distributed big data processing using Apache Spark on AWS EMR. This script processes the raw data, performs MapReduce operations, and calculates key metrics.

3. **Lambda-Based Processing (Optional)**:
   - `Lambda_process_weather_data.py` was an earlier attempt to use AWS Lambda for data transformation but is now considered less scalable for large data volumes.

4. **Data Backup**:
   - `Lambda_PushToAthena.py` provides a backup mechanism by storing raw data in S3, allowing analysis via Athena.

---

## **Getting Started**

1. **Simulate IoT Sensor Data**:
   - Use `api_simdata_gen.py` for API-based ingestion.
   - Use `kinesisagent_simdata_gen.py` for agent-based ingestion.

2. **Process the Data**:
   - Run `kinesis-spark-etl.py` on an AWS EMR cluster.

3. **Optional Steps**:
   - Use the Lambda functions as needed for lightweight processing or backups.

---
