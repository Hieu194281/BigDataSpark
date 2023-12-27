# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
# # import json

# # Create a Spark configuration
# conf = SparkConf().setAppName("KafkaToHadoop")
# # Create a StreamingContext with batch interval of 5 seconds
# ssc = StreamingContext(conf, 5)

# # Kafka broker details
# kafka_params = {
#     "bootstrap.servers": "kafka:9092",
#     "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
#     "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
#     "group.id": "your_consumer_group_id",
#     "auto.offset.reset": "latest"
# }
# topics = ["eventUserKafka"]

# # Create a Kafka DStream
# kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

# # Process the received data and write to Hadoop
# def process_and_save_to_hadoop(rdd):
#     # Modify the processing logic as per your requirements
#     spark = SparkSession.builder.appName("KafkaToHadoopWriter").getOrCreate()
#     processed_data = rdd.map(lambda x: x[1])  # Extracting the value from Kafka message
#     # Save processed data to Hadoop (HDFS)
#     processed_df = spark.createDataFrame(processed_data)
#     # processed_data.saveAsTextFiles("hdfs://81ba9cc4800c:8020/product/")
#     processed_df.write.mode('overwrite').json("hdfs://81ba9cc4800c:8020/product/product.json")

# # Apply processing and saving logic to the Kafka stream
# kafka_stream.foreachRDD(process_and_save_to_hadoop)

# # Start the streaming context
# ssc.start()
# ssc.awaitTermination()
try:
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    
    print("Kafka-related modules are accessible.")
except ImportError as e:
    print("Failed to import Kafka-related modules:", e)
