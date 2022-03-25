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