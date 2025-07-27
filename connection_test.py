import base64
import confluent_kafka
import datetime
import json
import os
import random
import socket


def nslookup(domain):
    try:
        ip = socket.gethostbyname(domain)
        print("domain: ", domain, " IP: ", ip)
    except socket.gaierror:
        print(f"Unable to resolve domain {domain}")


nslookup("bootstrap.my-kafka-cluster.us-central1.managedkafka.platform-workspace-a-001.cloud.goog")

kafka_topic_name = "my-kafka-topic"
bootstrap_hostname = 'bootstrap.my-kafka-cluster.us-central1.managedkafka.platform-workspace-a-001.cloud.goog:9092'

with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "rb") as f:
    result = base64.b64encode(f.read())
    password = result.decode('utf-8')

    conf = {
        'bootstrap.servers': bootstrap_hostname,
        'group.id': 'group1',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'kafka-producer-consumer@platform-workspace-a-001.iam.gserviceaccount.com',
        'sasl.password': password,

    }

producer = confluent_kafka.Producer(conf)

# Generate 10 random messages
for i in range(10):
    # Generate a random message
    now = datetime.datetime.now()
    datetime_string = now.strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "random_id": random.randint(1, 10600),
        "date_time": datetime_string
    }
    # Serialize data to bytes
    serialized_data = json.dumps(message_data).encode('utf-8')
    # Produce the message
    producer.produce(kafka_topic_name, serialized_data)
    print(f"Produced {i} messages")
    producer.flush()
producer.flush()

consumer = confluent_kafka.Consumer(conf)
consumer.subscribe([kafka_topic_name])

try:
    while (True):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            print("Something went wrong...")

        print(msg.value())
except KeyboardInterrupt:
    print('Canceled by user.')
finally:
    consumer.close()
