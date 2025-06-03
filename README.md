# Kafka File Processing Framework

This project provides a Python-based framework for processing files using Apache Kafka as a message broker. It consumes file data (content or metadata) from a Kafka topic, applies optional processing, and either saves the processed files to a destination directory or produces them to another Kafka topic. The framework is modular, extensible, and includes error handling and logging for robust operation.

## Features
- **Kafka Integration**: Consumes file data from a Kafka `source_topic` and optionally produces processed data to a `dest_topic`.
- **File Processing**: Supports custom processing of file content via a user-defined function.
- **Flexible Output**: Processed files can be saved to a local directory, sent to a Kafka topic, or both.
- **Error Handling**: Comprehensive error handling with logging to track processing and Kafka-related issues.
- **Modular Design**: Easily extendable for different file types, processing logic, or Kafka configurations.
- **Producer Script**: Includes an optional script to send files from a source directory to a Kafka topic for testing or integration.

## Directory Structure
```
project/
├── source/                     # Source directory with input files (optional)
│   ├── file1.txt               # Example input file
│   ├── file2.csv               # Example input file
├── destination/                # Destination directory for processed files (optional)
│   ├── file1.txt               # Processed output file
│   ├── file2.csv               # Processed output file
├── kafka_file_processing_framework.py  # Main framework script
├── kafka_producer.py           # Optional producer script to send files to Kafka
├── kafka_file_processing.log   # Log file for framework operations
```

## Prerequisites
- **Python**: Version 3.8 or higher.
- **Kafka**: A running Kafka cluster (e.g., on `localhost:9092`) with `source_topic` and (optionally) `dest_topic` created.
- **Dependencies**: Install the required Python package:
  ```bash
  pip install confluent-kafka
  ```

**Kafka Setup**: Ensure Kafka is running and accessible. Create topics using:

```bash
kafka-topics.sh --create --topic source_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic dest_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Installation

1. Clone or download this project to your local machine.
2. Set up the directory structure:

   ```bash
   mkdir -p project/source project/destination
   ```

3. Install the required Python package:

   ```bash
   pip install confluent-kafka
   ```

4. Ensure your Kafka cluster is running and topics are created.

## Configuration

Edit the `main()` function in `kafka_file_processing_framework.py` to configure:

```python
BOOTSTRAP_SERVERS = 'localhost:9092'
SOURCE_TOPIC = 'source_topic'
DEST_DIR = 'project/destination'
DEST_TOPIC = 'dest_topic'

framework = KafkaFileProcessingFramework(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    source_topic=SOURCE_TOPIC,
    dest_dir=DEST_DIR,
    dest_topic=DEST_TOPIC
)

framework.process_files(process_func=example_process_func)
```

## Usage

### Step 1: Prepare Input Files (Optional)

If using a source directory, place input files (e.g., `.txt`, `.csv`) in `project/source/`:

```
project/source/
├── file1.txt   # Contains: "Hello, world!"
├── file2.csv   # Contains: "name,age\nAlice,30"
```

### Step 2: Send Files to Kafka (Optional)

Use `kafka_producer.py`:

```bash
python project/kafka_producer.py
```

This reads files from `project/source/` and sends them as JSON messages:

```json
{
  "file_name": "file1.txt",
  "content": "Hello, world!"
}
```

### Step 3: Run the Framework

```bash
python project/kafka_file_processing_framework.py
```

- Consumes messages from `source_topic`.
- Applies processing function if provided.
- Saves to `DEST_DIR` and/or produces to `DEST_TOPIC`.

### Step 4: Verify Output

- Check `project/destination/` for processed files.
- Use a Kafka consumer to check `dest_topic`.
- Review `kafka_file_processing.log` for logs.

## Customizing Processing

Define a function:

```python
def custom_process_func(content: str) -> str:
    return content.upper()
```

Then use:

```python
framework.process_files(process_func=custom_process_func)
```

## Kafka Message Format

Messages from `source_topic` should look like:

```json
{
  "file_name": "file1.txt",
  "content": "Hello, world!"
}
```

## Notes

- **Text Files Only**: For binary files, use base64 encoding.
- **Kafka Config**: Customize consumer/producer configs as needed.
- **Error Logging**: See `kafka_file_processing.log`.
- **Scalable**: Use multiple consumers or Kafka partitions.

## Troubleshooting

- Kafka not reachable → Check `BOOTSTRAP_SERVERS`.
- No messages → Ensure topic has data.
- File not saved → Check `DEST_DIR` exists and is writable.
- Processing failed → See log for traceback.

## License

This project is provided as-is for educational and development purposes. Ensure compliance with Apache Kafka and `confluent-kafka` licensing terms.
