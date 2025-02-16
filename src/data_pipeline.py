from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Ecommerce Data Pipeline") \
    .getOrCreate()

# Load Data from a sample source (Replace with actual data source)
df = spark.read.option("header", "true").csv("../data/ecommerce_data.csv")

# Data Transformation Example
df_transformed = df.withColumn("total_price", col("price") * col("quantity"))

# Write to output storage (Replace with Azure Synapse, CosmosDB, or ADLS)
df_transformed.write.mode("overwrite").parquet("../data/transformed_data.parquet")

print("Data pipeline execution completed successfully.")
 
