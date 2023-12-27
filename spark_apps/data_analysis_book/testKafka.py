import os

spark_version = '3.3.4'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]


# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("KafkaStreamReader").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-10_3.0.0:3.3.3").getOrCreate()
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("KafkaStreamReader").getOrCreate()

# Read from Kafka topic
# df = spark \
#     .readStream \
#     .format("afka") \
#     .option("kafka.bootstrap.servers", "kafka2:9092") \
#     .option("subscribe", "baeldung_linux") \
#     .load()
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "kafka:9092,host2:9092") \
#   .option("subscribe", "baeldung_linux") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# from pyspark.sql import SparkSession
# spark = SparkSession \
#         .builder \
#         .appName("test") \
#         .config("spark.jars.packages", ",".join(packages))\
#         .getOrCreate()

# checkpoint_location = "hdfs://e44631f16eab:8020/product/"  # Choose your desired location
# spark.sparkContext.setCheckpointDir(checkpoint_location)

# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9092") \
#     .option("failOnDataLoss", "false") \
#     .option("subscribe", "product") \
#     .option("includeHeaders", "true") \
#     .option("startingOffsets", "latest") \
#     .option("spark.streaming.kafka.maxRatePerPartition", "50") \
#     .load()
# # To read from Kafka for streaming queries, we can use the function spark.readStream. We use the spark session we had created to read stream by giving the Kafka configurations like the bootstrap servers and the Kafka topic on which it should listen. We can…



# # Convert value column from Kafka to string (assuming value is encoded in bytes)
# # df = df.selectExpr("CAST(value AS STRING)")

# # Start the streaming query
# # query = kafka_df.writeStream.outputMode("append").format("console").start()  # Change the output format as needed

    # query = kafka_df.writeStream \
    #     .outputMode("append") \
    #     .format("json") \
    #     .option("path", "hdfs://e44631f16eab:8020/product/product.json") \
    #     .option("checkpointLocation", checkpoint_location) \
    #     .start()
# query.awaitTermination()

# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# # Create a SparkContext
# spark = SparkSession.builder.appName("KafkaToHadoopWriter").getOrCreate()
# # sc = SparkContext(appName="KafkaToHadoopWriter")

# # Create a StreamingContext
# ssc = StreamingContext(spark.SparkContext, 5)  # 5 seconds batch interval

# # Kafka parameters
# kafka_params = {
#     "bootstrap.servers": "kafka:9092",
#     "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
#     "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
#     "auto.offset.reset": "latest"
# }
# topics = ["product"]

# # Create a Kafka DStream
# # kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)
# kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()


# # Process the received data and write to Hadoop
# def save_to_hadoop(rdd):
#     if not rdd.isEmpty():
#         # Convert RDD to DataFrame if needed and perform necessary processing
#         # Example: Assuming the data is in JSON format and you want to save it as text files in HDFS
#         dataframe = spark.read.json(rdd.map(lambda x: x[1]))  # Convert RDD to DataFrame
#         dataframe.write.mode('overwrite').json("hdfs://e44631f16eab:8020/product/product.json")

# # Apply processing logic to the Kafka stream
# kafka_stream.foreachRDD(save_to_hadoop)

# # Start the StreamingContext
# ssc.start()
# ssc.awaitTermination()

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaToHadoopWriter").getOrCreate()

# Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "product",
    "kafka.key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "kafka.value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id": "your_consumer_group_id",
    "auto.offset.reset": "latest"
}

# Define the Kafka source for streaming
kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()

# Processing logic to write to Hadoop
def write_to_hadoop(df, epoch_id):
    if not df.isEmpty():
        # Assuming the received data is in JSON format, write it to HDFS
        df.write.mode('overwrite').json("hdfs://e44631f16eab:8020/product/product.json")

# Apply processing logic to the streaming DataFrame
query = kafka_df.writeStream.foreachBatch(write_to_hadoop).start()

# Start the streaming query
query.awaitTermination()


