import confluent_kafka
import datetime
import random
import json

# This example uses Confluent's Python Client for Apache Kafka
# https://github.com/confluentinc/confluent-kafka-python
# Update to your clusters specific metadata
kafka_cluster_name = ''
region = ''
project_id = ''
port = ''
kafka_topic_name = "my-kafka-topic"
bootstrap_hostname = 'bootstrap.my-kafka-cluster.us-central1.managedkafka.platform-workspace-a-001.cloud.goog:9092'

# Set up the config for Kafka
# Using OAUTHBEARER mechanism and local auth server
conf = {
  'bootstrap.servers': bootstrap_hostname,
  'group.id': 'group1',
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'OAUTHBEARER',
  'sasl.oauthbearer.token.endpoint.url': 'localhost:14293',
  'sasl.oauthbearer.client.id': 'unused',
  'sasl.oauthbearer.client.secret': 'unused',
  'sasl.oauthbearer.method': 'oidc'
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
