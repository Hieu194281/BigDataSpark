from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("WriteDFToHDFS").getOrCreate()

# Sample data - a list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["name", "age"])

# Write the DataFrame as a text file in HDFS
df.write.text("hdfs://172.18.0.5:9866/data/test.txt")