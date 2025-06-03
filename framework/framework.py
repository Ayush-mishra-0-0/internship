import os
import shutil
import logging
from typing import Callable
import preprocess  # Import your preprocessing module (replace with your module name)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='file_processing.log'
)

class FileProcessingFramework:
    def __init__(self, source_dir: str, dest_dir: str, preprocess_func: Callable):
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.preprocess_func = preprocess_func

        # Validate directories
        self._validate_directories()

    def _validate_directories(self):
        if not os.path.exists(self.source_dir):
            logging.error(f"Source directory {self.source_dir} does not exist.")
            raise FileNotFoundError(f"Source directory {self.source_dir} does not exist.")
        if not os.path.exists(self.dest_dir):
            logging.info(f"Creating destination directory {self.dest_dir}")
            os.makedirs(self.dest_dir)

    def process_files(self, file_extensions: list = None):
        try:
            files = os.listdir(self.source_dir)
            if file_extensions:
                files = [f for f in files if os.path.splitext(f)[1].lower() in file_extensions]

            if not files:
                logging.warning(f"No files found in {self.source_dir} with extensions {file_extensions}")
                return

            for file_name in files:
                source_path = os.path.join(self.source_dir, file_name)
                dest_path = os.path.join(self.dest_dir, file_name)

                if os.path.isfile(source_path):
                    self._process_single_file(source_path, dest_path, file_name)

        except Exception as e:
            logging.error(f"Error processing files: {str(e)}")
            raise

    def _process_single_file(self, source_path: str, dest_path: str, file_name: str):
        try:
            logging.info(f"Processing file: {file_name}")

            with open(source_path, 'r', encoding='utf-8') as file:
                content = file.read()

            processed_content = self.preprocess_func(content)

            with open(dest_path, 'w', encoding='utf-8') as file:
                file.write(processed_content)

            logging.info(f"Successfully processed and saved: {file_name}")
        except Exception as e:
            logging.error(f"Error processing {file_name}: {str(e)}")
            raise

def main():
    SOURCE_DIR = "path/to/source"
    DEST_DIR = "path/to/destination"
    FILE_EXTENSIONS = ['.txt', '.csv']

    framework = FileProcessingFramework(
        source_dir=SOURCE_DIR,
        dest_dir=DEST_DIR,
            preprocess_func=preprocess.preprocess_text  
        )

    framework.process_files(file_extensions=FILE_EXTENSIONS)

if __name__ == "__main__":
    main()