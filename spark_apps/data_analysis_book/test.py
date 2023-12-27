from hdfs import InsecureClient

# Kết nối đến HDFS
hdfs_client = InsecureClient('http://localhost:9870', user='root')

# Đường dẫn trên HDFS để đọc dữ liệu
# hdfs_path = '/user/root/TKB20231-FULL-1809.xlsx'
hdfs_path = '/data/test.txt'

# Đọc nội dung từ file trên HDFS
with hdfs_client.read(hdfs_path) as reader:
    content = reader.read()

# Hiển thị nội dung
print(content.decode('utf-8'))