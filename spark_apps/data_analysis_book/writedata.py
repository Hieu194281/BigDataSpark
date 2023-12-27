# from hdfs import InsecureClient
# import pandas as pd
# liste_hello = ['hello1','hello2']
# liste_world = ['world1','world2']
# df = pd.DataFrame(data = {'hello' : liste_hello, 'world': liste_world})
# hdfs_client = InsecureClient('http://localhost:9863', user='root')
# with hdfs_client.write('/path/in/hdfs/destination.txt', encoding='utf-8') as writer:
#     writer.write(data_to_write)

from hdfs import InsecureClient

data_to_write = "Hello, Hadoop! This is some data to write."

hdfs_client = InsecureClient('http://localhost:9870', user='root')

with hdfs_client.write('/data/destination.txt', encoding='utf-8') as writer:
    writer.write(data_to_write)