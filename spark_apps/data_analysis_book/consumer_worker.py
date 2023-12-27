from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from hdfs import InsecureClient
import json
import time

MAPPING_ACTION_TO_POINT = {
    'search': 1,
    'view': 3,
    'add_to_cart': 10,
    'buy': 15
}


def create_topic(bootstrap_servers, topic_name, partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Define the new topic with the specified parameters
    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)

    # Create the topic
    fs = admin_client.create_topics([new_topic])

    # Wait for topic creation to finish
    for topic, f in fs.items():
        try:
            f.result()  # Raises exception on failure
            print(f"Topic '{topic}' created successfully!")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

    admin_client = None

def topic_exists(bootstrap_servers, topic_name):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Fetch existing topics
    metadata = admin_client.list_topics(timeout=10)

    # Check if the topic exists in the metadata
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' exists in Kafka!")
        return True
    else:
        print(f"Topic '{topic_name}' does not exist in Kafka.")
        return False

    # Close the AdminClient
    admin_client = None

def checkExistFolder(client, pathCheck): 
    if(client.status(pathCheck, strict=False)):
        return True
    else: return False

def write_to_hdfs(needed_information):
    hdfs_client = InsecureClient('http://localhost:9870', user='root')
    try: 
        data = json.dumps(needed_information)  # Convert to JSON string
        data = data.encode('utf-8')  # Convert to bytes
        dir_path = '/product'
        timestamp = time.time()
        # hdfs_client.set_permission('/', permission=0o777)
        # hdfs_client.chmod('/', 0o777)
        # if(not checkExistFolder(hdfs_client, dir_path)): 
        #     hdfs_client.makedirs(dir_path)
        #     # hdfs_client.chmod(dir_path, 0o777)
        #     hdfs_client.set_permission(dir_path, permission=0o777)
        #     print(f"Directory '{dir_path}' created successfully in HDFS.")

        file_path = f'/product/product_{str(timestamp)}.json'
        
        with hdfs_client.write(file_path) as writer:
            writer.write(data)
    except Exception as e: 
        print(f'Error when writing to HDFS: {e}')

if(not topic_exists('localhost:29092', 'product')): 
    create_topic('localhost:29092', 'product', 4)

c = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})

c.subscribe(['product'])

print("Recommend Consumer Starting")
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # When reading is reaching end of file, ignore it
            continue
        else:
            print("Consumer error: {}".format(msg.error()))
            break

    print('Received message: {}'.format(msg.value().decode('utf-8')))
    try:
        # Unmarshall message event in kafka to dictionary
        message = json.loads(msg.value().decode('utf-8'))
        products = message.get("products", [])

        for product in products:
            # If product item does not have 'categories' field, skip it
            if "categories" not in product:
                continue
            categories = product.get("categories", {})

            # If 'categories' isn't a leaf one, skip it
            categoriesDict = eval(categories.replace("'", "\""))
            if not categoriesDict.get("is_leaf", False):
                continue
            else :
            # Format message to store into HDFS
                needed_information = {
                    "user_id": message.get("user_id", 0),
                    "category_id": categoriesDict.get("id"),
                    "point": MAPPING_ACTION_TO_POINT[message.get("action", "search")],
                    "timestamp": message.get("timestamp", time.time()),
                }   
                write_to_hdfs(needed_information)
                print(f'Completed writing to HDFS message: {needed_information}')
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        continue
    except Exception as e:
        print(f"Unknown error: {e}")
        continue

c.close()