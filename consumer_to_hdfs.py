import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

# 1. Koneksi ke HDFS via WebHDFS API
hdfs_client = InsecureClient('http://localhost:9870', user='root')

# 2. Buat folder terpisah di HDFS jika belum ada
hdfs_client.makedirs('/data/cuaca')
hdfs_client.makedirs('/data/kualitas_udara')

# 3. Koneksi ke Kafka Consumer untuk 2 topic
consumer = KafkaConsumer(
    'data-cuaca', 'data-kualitas-udara',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka Consumer aktif... Menunggu data masuk dan merapikan ke HDFS.")

counter_cuaca = 1
counter_udara = 1

try:
    for message in consumer:
        topic_name = message.topic
        data = message.value
        
        # Tentukan folder tujuan & penamaan file berdasarkan nama topic
        if topic_name == 'data-cuaca':
            folder_hdfs = '/data/cuaca'
            filename = f'cuaca_{counter_cuaca}.json'
            counter_cuaca += 1
        else:
            folder_hdfs = '/data/kualitas_udara'
            filename = f'kualitas_udara_{counter_udara}.json'
            counter_udara += 1

        hdfs_path = f'{folder_hdfs}/{filename}'
        
        # Simpan file ke HDFS
        with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            json.dump(data, writer)
            
        print(f"[{topic_name}] --> Disimpan rapi ke: {hdfs_path}")

except KeyboardInterrupt:
    print("\nConsumer dihentikan.")