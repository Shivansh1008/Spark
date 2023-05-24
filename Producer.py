from confluent_kafka import Producer
import csv

API_KEY = 'KI3UKMZVFHR72PPX'
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = '1WHoM+Rhb56T9+b1CnwIAHZBjSlqbJVQU+LFQxz6c+24aK+Mr/i1GfB2l3HW8vpE'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'WIAZVKDD6D3EI3W4'
SCHEMA_REGISTRY_API_SECRET = 'DPkEt3ZvqKpAO3pVOiOkD519ezjrGYMXBt1B1qQf1Bnhq1yByZ2acozB1BFMAHv9'


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_csv_to_kafka(bootstrap_servers, topic, csv_files):
    # Create producer configuration
    conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }

    # Create Kafka producer
    producer = Producer(conf)

    for csv_file in csv_files:
        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)  # Read headers from the CSV file

            for row in csv_reader:
                # Convert CSV row to a string
                csv_row = ','.join(row)

                # Publish the CSV row as a message to the Kafka topic
                producer.produce(topic, value=csv_row.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
    producer.flush()

    # Close the Kafka producer
    producer.close()

# Example usage
bootstrap_servers = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'  # Kafka bootstrap servers (e.g., 'localhost:9092')
topic = 'Corona'  # Kafka topic to publish CSV data
csv_files = ['/config/workspace/archive/Case.csv']  # List of CSV files to stream

publish_csv_to_kafka(bootstrap_servers, topic, csv_files)
