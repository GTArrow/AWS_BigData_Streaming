from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit, udf, expr
import json
import math
from datetime import datetime
import sys

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

def print_kinesis_data(rdd):
    """
    Print each record received from Kinesis.
    """
    print("Printing Stream.....")
    print("Number of record from Kinesis: "+ str(rdd.count()))
    if not rdd.isEmpty():
        print("=== Batch Received ===")
        records = rdd.collect()
        for record in records:
            print(json.loads(record)) 

def process_kinesis_stream(spark, rdd, output_path):
    """
    Process each RDD in the DStream, calculate FFWI, and save as Parquet to S3 partitioned by year/month/day.
    """
    print("Processing Stream.....")
    print("Number of RDD in DStream: "+ str(rdd.count()))
    try:
        if not rdd.isEmpty():
            # Extract: Parse records as JSON
            parsed_records = rdd.map(lambda record: json.loads(record))

            # Filter out extreme values (temperature < -80 or > 80, humidity < 0 or > 100, wind_speed < 0 or > 50)
            filtered_records = parsed_records.filter(
                lambda record: -80 <= record["temperature"] <= 80
                and 0 <= record["humidity"] <= 100
                and 0 <= record["windSpeed"] <= 50
            )

            # Transform: Convert RDD to DataFrame
            schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("windSpeed", DoubleType(), True)
            ])
            df = spark.createDataFrame(filtered_records, schema=schema)

            # Add calculated FFWI and partition columns
            df = df.withColumn("ffwi", udf_calculate_ffwi(df["temperature"], df["humidity"], df["windSpeed"]))

            # Map: Add logic for grouping data by region (latitude and longitude)
            df = df.withColumn("region", expr("concat(round(latitude, 1), '_', round(longitude, 1))"))

            # Reduce: Perform aggregations
            aggregated_df = (
                df.groupBy("region")
                .agg(
                    expr("percentile_approx(windSpeed, 0.5)").alias("MedianWindSpeed"),
                    expr("percentile_approx(temperature, 0.5)").alias("MedianTemperature"),
                    expr("percentile_approx(humidity, 0.5)").alias("MedianHumidity"),
                    expr("avg(ffwi)").alias("AverageFFWI")
                )
            )

            aggregated_df.printSchema()
            print(aggregated_df.show())
            print("Total number of records: " + str(aggregated_df.count()))
            # Load: Write aggregated DataFrame to S3 as Parquet with partitioning
            aggregated_df.write.mode("overwrite").partitionBy("timestamp").parquet(output_path)
    except Exception as e:
        print(f"Error processing stream: {e}")

def debug_kinesis_stream(rdd):
    print(f"RDD Partition Count: {rdd.getNumPartitions()}")
    print(f"RDD Record Count: {rdd.count()}")
    if not rdd.isEmpty():
        print("First Record:", rdd.first())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-kinesis-etl [output-folder]")
        sys.exit(-1)

    # S3 output path
    output_path = sys.argv[1]
    print("Initialize Spark context and streaming context")
    # Initialize Spark context and streaming context
    sc = SparkContext("local[2]", "KinesisWeatherDataProcessing")
    sc.setLogLevel("DEBUG")
    ssc = StreamingContext(sc, 10)  # Batch interval: 10 seconds

    spark = SparkSession.builder.getOrCreate()

    # Register UDF for FFWI calculation
    from pyspark.sql.functions import udf, lit
    from pyspark.sql.types import FloatType
    udf_calculate_ffwi = udf(calculate_ffwi, FloatType())
    print("Create Kinesis DStream")
    # Create Kinesis DStream
    kinesis_stream = KinesisUtils.createStream(
        ssc,
	    kinesisAppName="KinesisWeatherDataProcessing",
        streamName="weather_data_stream",
        endpointUrl="https://kinesis.us-east-2.amazonaws.com",
        regionName="us-east-2",
        initialPositionInStream=InitialPositionInStream.TRIM_HORIZON,
        checkpointInterval=10,
        awsAccessKeyId = "HideKeyID",
        awsSecretKey = "HideSecreteKey"
    )

    #kinesis_stream.foreachRDD(debug_kinesis_stream)

    print("Stream Created; Printing Stream.....")
    # Print raw Kinesis data
    print("Processing Stream.....")
    # Process the Kinesis stream
    kinesis_stream.foreachRDD(lambda rdd: process_kinesis_stream(spark, rdd, output_path))

    print("Start the context...")
    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()

