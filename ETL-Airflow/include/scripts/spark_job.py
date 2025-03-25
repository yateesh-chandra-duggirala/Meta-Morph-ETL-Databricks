from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("SparkProcessing").getOrCreate()

# Load Data (Example CSV)
df = spark.read.csv('/usr/local/airflow/include/data.csv', header=True, inferSchema=True)

# Perform transformations (Example: Select specific columns)
# transformed_df = df.select("id", "name", "age")

# Save the transformed data to CSV (or any other format)
# df.write.csv('/usr/local/airflow/include/output_data.csv', header=True, mode="overwrite")
df.show()

print("Spark processing completed!")
