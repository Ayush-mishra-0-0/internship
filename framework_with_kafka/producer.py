from confluent_kafka import Producer
import json
import os
import logging

def send_files_to_kafka(bootstrap_servers: str, topic: str, source_dir: str):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    
    for file_name in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file_name)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                message = {
                    'file_name': file_name,
                    'content': content
                }
                producer.produce(topic, value=json.dumps(message).encode('utf-8'))
                producer.flush()
                logging.info(f"Sent file {file_name} to {topic}")
            except Exception as e:
                logging.error(f"Error sending {file_name}: {str(e)}")

# Usage
send_files_to_kafka('localhost:9092', 'source_topic', 'path/to/source')