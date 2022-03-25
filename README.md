# ids721-project3
## Overview
Cloud-based Big Data Systems Project
Use a major Big Data system to perform a Data Engineering related task
Example systems could be: (AWS Athena, AWS Spark/EMR, AWS Sagemaker, Databricks, Snowflake)

I created a Cloud-based Big Data Systems Project on AWS Spark/EMR. The project is handling weather data ("stationID", "date", "measure_type", "temperature") to find the min temperature (in "measure_type": TMIN) grouped by station.

datasource: weather_data.csv
spark process code using pyspark: main.py


## Create AWS Spark/EMR cluster
<img width="641" alt="image" src="https://user-images.githubusercontent.com/47130690/160037583-1764c198-f7d5-4cea-9297-01e302d81378.png">


## Create S3 bucket to contain data source
<img width="615" alt="image" src="https://user-images.githubusercontent.com/47130690/160037673-b6857e32-cc33-42a3-9166-bf03b1b93fa7.png">
<img width="616" alt="image" src="https://user-images.githubusercontent.com/47130690/160037721-07e43023-799c-44f3-91e8-0c1d1d96db20.png">
<img width="616" alt="image" src="https://user-images.githubusercontent.com/47130690/160037734-c50422a7-1066-4e2f-818e-4241ffa1af8f.png">
create a folder to contain the data source .csv file
<img width="617" alt="image" src="https://user-images.githubusercontent.com/47130690/160037782-3c2310c4-707f-49ab-9bac-8d3017bfedd7.png">
<img width="615" alt="image" src="https://user-images.githubusercontent.com/47130690/160037828-ca9aec36-a08d-4459-a00f-cc09538c6bfd.png">
upload weather-data.csv file.  

<img width="618" alt="image" src="https://user-images.githubusercontent.com/47130690/160037870-2fbfb1d9-34d5-4b7b-8e21-8730854e9aca.png">

## Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

S3_DATA_SOURCE_PATH = 's3://temporature-data-bucket/data-source/weather-data.csv'
S3_DATA_OUTPUT_PATH = 's3://temporature-data-bucket/data-output'


def main():
	spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

	schema = StructType([ \
	                     StructField("stationID", StringType(), True), \
	                     StructField("date", IntegerType(), True), \
	                     StructField("measure_type", StringType(), True), \
	                     StructField("temperature", FloatType(), True)])

	# // Read the file as dataframe
	df = spark.read.schema(schema).csv(S3_DATA_SOURCE_PATH)
	df.printSchema()

	# Filter out all but TMIN entries
	minTemps = df.filter(df.measure_type == "TMIN")

	# Select only stationID and temperature
	stationTemps = minTemps.select("stationID", "temperature")

	# Aggregate to find minimum temperature for every station
	minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
	minTempsByStation.show()

	# Convert temperature to fahrenheit and sort the dataset
	minTempsByStationF = minTempsByStation.withColumn("temperature",
	                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
	                                                  .select("stationID", "temperature").sort("temperature")
	                                                  
	# Write result to S3 bucket
	minTempsByStationF.rdd.saveAsTextFile(S3_DATA_OUTPUT_PATH)
	

	# Collect, format, and print the results
	results = minTempsByStationF.collect()

	for result in results:
	    print(result[0] + "\t{:.2f}F".format(result[1]))

	print('Selected data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH)
	    
	spark.stop()

if __name__ == '__main__':
	main()
```

## SSH to EMR cluster
Now the EMR cluster is ready:  

<img width="623" alt="image" src="https://user-images.githubusercontent.com/47130690/160038038-685fe791-1817-4d02-bcbd-ef7f272f6aff.png">   

add port 20 ssh to master node's security group  


ssh to EMR cluster  

<img width="603" alt="image" src="https://user-images.githubusercontent.com/47130690/160038156-b6c8b876-01f2-4c5f-b0cd-266cdfe01eca.png">

## spark job on EMR cluster
```bash
$ vi main.py
# then copy all code and paste to the main.py file, save

$ spark-submit main.py
# now spark start executing the main.py
```

## check result
in terminal print-out:    

<img width="580" alt="image" src="https://user-images.githubusercontent.com/47130690/160038403-267801ee-5fe5-493d-a520-75fbbe637825.png">
<img width="608" alt="image" src="https://user-images.githubusercontent.com/47130690/160038383-c9b64ae0-ee2c-479c-90af-1a94b9329d5b.png">
   
   

in S3 bucket:   

<img width="605" alt="image" src="https://user-images.githubusercontent.com/47130690/160038348-4860c108-871f-43ee-af03-69cffa9694fb.png">
<img width="603" alt="image" src="https://user-images.githubusercontent.com/47130690/160038314-0bc5bbf5-df25-453e-acf1-8d63a5f8e892.png">

