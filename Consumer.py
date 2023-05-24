from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient

bootstrap_servers = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'  # Kafka bootstrap servers (e.g., 'localhost:9092')
group_id = 'group1'  # Kafka consumer group ID
topic = 'Corona'  # Kafka topic to consume from
mongodb_uri = 'mongodb://localhost:27017/'  # MongoDB URI
mongodb_database = 'Pyspark_ass'  # MongoDB database name
mongodb_collection = 'case_kafka'  # MongoDB collection name
API_KEY = 'KI3UKMZVFHR72PPX'
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = '1WHoM+Rhb56T9+b1CnwIAHZBjSlqbJVQU+LFQxz6c+24aK+Mr/i1GfB2l3HW8vpE'
#BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'




def consume_and_store_kafka_data(bootstrap_servers, group_id, topic, mongodb_uri, mongodb_database, mongodb_collection):
    # Create Kafka consumer configuration
    conf = {
        'sasl.mechanism': SSL_MACHENISM,
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'group1',
        'auto.offset.reset': 'earliest',
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.username': 'KI3UKMZVFHR72PPX',
        'sasl.password': '1WHoM+Rhb56T9+b1CnwIAHZBjSlqbJVQU+LFQxz6c+24aK+Mr/i1GfB2l3HW8vpE'
    }

    # Create Kafka consumer
    consumer = Consumer(conf)

    # Subscribe to Kafka topic
    consumer.subscribe([topic])

    # Create MongoDB client and connect to the database
    client = MongoClient(mongodb_uri)
    db = client[mongodb_database]
    collection = db[mongodb_collection]

    try:
        while True:
            # Poll for Kafka messages
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaException._PARTITION_EOF:
                    # Reached the end of partition, continue polling
                    continue
                else:
                    # Handle other Kafka exceptions
                    print(f"Kafka error: {message.error()}")
                    break

            # Extract the value from the Kafka message
            value = message.value().decode('utf-8')

            # Store the message in MongoDB
            collection.insert_one({'message': value})

    except KeyboardInterrupt:
        # Stop consuming on keyboard interrupt
        pass

    # Close the Kafka consumer and MongoDB connection
    consumer.close()
    client.close()

# Example usage





consume_and_store_kafka_data(bootstrap_servers, group_id, topic, mongodb_uri, mongodb_database, mongodb_collection)
