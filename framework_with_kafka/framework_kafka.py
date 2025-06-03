import os
import logging
from confluent_kafka import Consumer, Producer, KafkaException
import json
from typing import Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='kafka_file_processing.log'
)

class KafkaFileProcessingFramework:
    def __init__(self, bootstrap_servers: str, source_topic: str, dest_dir: str = None, 
                 dest_topic: str = None, group_id: str = 'file_processor_group'):
        """
        Initialize the Kafka file processing framework.

        Args:
            bootstrap_servers (str): Kafka bootstrap servers (e.g., 'localhost:9092').
            source_topic (str): Kafka topic to consume files from.
            dest_dir (str, optional): Destination directory to save processed files.
            dest_topic (str, optional): Kafka topic to produce processed files to.
            group_id (str): Kafka consumer group ID.
        """
        self.bootstrap_servers = bootstrap_servers
        self.source_topic = source_topic
        self.dest_dir = dest_dir
        self.dest_topic = dest_topic
        self.group_id = group_id

        # Validate inputs
        if not (dest_dir or dest_topic):
            raise ValueError("Either dest_dir or dest_topic must be provided.")
        if dest_dir:
            self._validate_directory(dest_dir)

        # Initialize Kafka consumer
        self.consumer = self._create_consumer()
        # Initialize Kafka producer (if dest_topic is provided)
        self.producer = self._create_producer() if dest_topic else None

    def _validate_directory(self, dest_dir: str):
        """Ensure destination directory exists."""
        if not os.path.exists(dest_dir):
            logging.info(f"Creating destination directory {dest_dir}")
            os.makedirs(dest_dir)

    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer."""
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([self.source_topic])
        return consumer

    def _create_producer(self) -> Producer:
        """Create and configure Kafka producer."""
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers
        }
        return Producer(producer_config)

    def _delivery_report(self, err, msg):
        """Callback for Kafka producer delivery reports."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

    def process_files(self, process_func: Callable = None):
        """
        Consume messages from source_topic, process them, and save/produce results.

        Args:
            process_func (Callable, optional): Function to process file content or metadata.
                                              If None, assumes message is file content and saves as-is.
        """
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Decode Kafka message (assuming JSON with file_name and content)
                    message = json.loads(msg.value().decode('utf-8'))
                    file_name = message.get('file_name', 'default_file.txt')
                    content = message.get('content', '')

                    logging.info(f"Received file: {file_name}")

                    # Apply processing function if provided
                    processed_content = process_func(content) if process_func else content

                    # Save to destination directory if specified
                    if self.dest_dir:
                        dest_path = os.path.join(self.dest_dir, file_name)
                        with open(dest_path, 'w', encoding='utf-8') as file:
                            file.write(processed_content)
                        logging.info(f"Saved processed file to {dest_path}")

                    # Produce to destination topic if specified
                    if self.dest_topic and self.producer:
                        output_message = {
                            'file_name': file_name,
                            'content': processed_content
                        }
                        self.producer.produce(
                            self.dest_topic,
                            value=json.dumps(output_message).encode('utf-8'),
                            callback=self._delivery_report
                        )
                        self.producer.flush()
                        logging.info(f"Produced processed file to {self.dest_topic}")

                    # Commit the message offset
                    self.consumer.commit()

                except Exception as e:
                    logging.error(f"Error processing message for {file_name}: {str(e)}")

        except KeyboardInterrupt:
            logging.info("Shutting down consumer")
            self.consumer.close()
            if self.producer:
                self.producer.flush()
        except Exception as e:
            logging.error(f"Error in processing loop: {str(e)}")
            raise

def example_process_func(content: str) -> str:
    """
    Example processing function (replace with your logic).
    
    Args:
        content (str): Input content from Kafka message.
    
    Returns:
        str: Processed content.
    """
    return content.upper()  # Example: Convert content to uppercase

def main():
    # Configuration
    BOOTSTRAP_SERVERS = 'localhost:9092'
    SOURCE_TOPIC = 'source_topic'
    DEST_DIR = 'path/to/destination'  # Optional: Set to None if not saving to disk
    DEST_TOPIC = 'dest_topic'  # Optional: Set to None if not producing to Kafka

    # Initialize framework
    framework = KafkaFileProcessingFramework(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        source_topic=SOURCE_TOPIC,
        dest_dir=DEST_DIR,
        dest_topic=DEST_TOPIC
    )

    # Process files with an optional processing function
    framework.process_files(process_func=example_process_func)

if __name__ == "__main__":
    main()