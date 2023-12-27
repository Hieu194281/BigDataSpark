from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_spark, rank, collect_list
from pyspark.sql.window import Window
import time

# Initialize SparkSession with Hadoop configurations
spark = SparkSession.builder \
    .appName("ReadFromHDFS") \
    .config("fs.defaultFS", "hdfs://be4ca181aaea:8020") \
    .getOrCreate()
input_folder = '/product/'

# Read the text file from HDFS into a DataFrame
# df = spark.read.text("hdfs://d8a89445348f:8020/data/part-00000-0c6e191a-3730-4a00-bea6-d3bff3e81325-c000.snappy.parquet")
df = spark.read.format("json") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(input_folder)

# Show the DataFrame contents
df.printSchema()
one_week_ago_timestamp = (time.time() - (7*24*60*60)) * 1000
df = df.where(f"timestamp > {one_week_ago_timestamp}").groupBy("user_id", "category_id").agg(sum_spark("point").alias("total_points"))
#get top2 category by totalpoint of userid

windowSpec = Window.partitionBy("user_id").orderBy(df["total_points"].desc())
ranked_df = df.withColumn("rank", rank().over(windowSpec))
top_two_records = ranked_df.filter(ranked_df["rank"] <= 2).drop("rank")

grouped_df = top_two_records.groupby("user_id").agg(collect_list("category_id").alias("category_array"))
grouped_df.show()
def process_row(row):
    user_id = row['user_id']
    category_id = row['category_id']
    total_points = row['total_points']
    
    # Perform operations or actions on each row
    print(f"Processing user_id: {user_id}, category_id: {category_id}, total_points: {total_points}")
    # Perform additional actions here...

# Apply the process_row function to each row using foreach
# df.foreach(process_row)
# rows = top_two_records.collect()
# for row in rows:
#     # Access columns by name or index in the row
#     user_id = row['user_id']
#     category_id = row['category_id']
#     total_points = row['total_points']
    
#     # Perform operations on each row
#     print(f"Processing user_id: {user_id}, category_id: {category_id}, total_points: {total_points}")
    # Perform additional actions here...
# top_two_records.show()
# df.show()

# Stop the SparkSession (optional, depending on your use case)
spark.stop()

def handle(data_frame):
    one_week_ago_timestamp = time.time() - (7 * 24 * 60 * 60)
    working_data_frame = extract_data_since(one_week_ago_timestamp)

    list_user = list_user_from_data_frame(working_data_frame)


def extract_data_since(timestamp):
    pass

def list_user_from_data_frame(data_frame):
    pass

def ranking_categories(data_frame, user_id):
    pass

def get_random_recommended_product(category_id):
    pass

def write_to_persist_datastore(endpoint, data, user_id):
    pass