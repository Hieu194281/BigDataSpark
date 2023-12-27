from pyspark.sql import SparkSession
from pyspark.sql.functions import concat

# Initialize SparkSession
spark = SparkSession.builder.appName("WriteDFToHDFS").config("fs.defaultFS", "hdfs://803c01b54138:8020").getOrCreate()

# Sample data - a list of tuples
data = [("Alice", 25    ), ("Bob", 30), ("Charlie", 35)]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["name", "age"])
# df = df.withColumn("age", col("age").cast("string"))
df = df.withColumn("combined", concat("name", "age"))

# Write the DataFrame as a text file in HDFS
df.select("combined").write.text("/data/test.txt")
spark.stop()