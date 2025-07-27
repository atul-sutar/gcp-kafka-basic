import base64
import datetime
import json
import os
import random
import socket
import sys

import confluent_kafka


def check_environment(kafka_url, kafka_service_account):
    try:
        ip = socket.gethostbyname(kafka_url.split(":")[0])
        print("domain: ", kafka_url, " IP: ", ip)

        with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "rb") as f:
            result = base64.b64encode(f.read())
            password = result.decode('utf-8')

            conf = {
                'bootstrap.servers': kafka_url,
                'group.id': 'group1',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': kafka_service_account,
                'sasl.password': password,

            }
        return conf
    except socket.gaierror:
        print(f"Unable to resolve domain {kafka_url}")
        sys.exit(1)
    except Exception as e:
        print(f"Error Occured: {e}")
        sys.exit(1)


def kafka_produce(configuration, topic_name):
    producer = confluent_kafka.Producer(configuration)

    # Generate 5 random messages
    for i in range(5):
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
        producer.produce(topic_name, serialized_data)
        print(f"Produced {i} messages")
        producer.flush()
    producer.flush()


def kafka_consume(configuration, topic_name):
    consumer = confluent_kafka.Consumer(configuration)
    consumer.subscribe([topic_name])
    print("Consumer running...")
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


if __name__ == "__main__":
    kafka_input_topic_name = "kafka-input"
    kafka_output_topic_name = "kafka-output"
    kafka_boostrap_url = 'bootstrap.kafka-cluster.us-central1.managedkafka.platform-workspace-a-001.cloud.goog:9092'
    kafka_sa_account = 'kafka-producer-consumer@platform-workspace-a-001.iam.gserviceaccount.com'

    print("Checking the connection and credentials")
    config = check_environment(kafka_url=kafka_boostrap_url, kafka_service_account=kafka_sa_account)

    print(f"Producing messages to output topic {kafka_output_topic_name}")
    kafka_produce(configuration=config, topic_name=kafka_output_topic_name)
    print("Messages produced successfully")
    print()

    print(f"Consuming messages to input topic {kafka_input_topic_name}")
    kafka_consume(configuration=config, topic_name=kafka_input_topic_name)
    print()
