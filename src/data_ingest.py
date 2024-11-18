import os
import logging
import zipfile
from abc import ABC, abstractmethod

import pandas as pd

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Abstract method to ingest data from a given file."""
        pass


class ZipDataIngestor(DataIngestor):
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Extracts a .zip file and returns the content as a pandas DataFrame.
        
        Args:
            file_path (str): The path to the .zip file to extract.
        
        Returns:
            pd.DataFrame: The content of the .zip file as a pandas DataFrame.
        """
        
        if not file_path.endswith(".zip"):
            raise ValueError("The provided file is not a .zip file.")

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall("../extracted_data")

        extracted_files = os.listdir("../extracted_data")
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if len(csv_files) == 0:
            raise FileNotFoundError("No CSV files found in the extracted data.")
        if len(csv_files) > 1:
            raise ValueError("Multiple CSV files found in the extracted data.")

        csv_file_path = os.path.join("../extracted_data", csv_files[0])
        df = pd.read_csv(csv_file_path)

        return df


class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(file_extension: str) -> DataIngestor:
        """Returns the appropriate DataIngestor based on file extension.
        
        Args:
            file_extension (str): The file extension of the data file.
        
        Returns:
            DataIngestor: An instance of the appropriate DataIngestor class.
        """
        
        if file_extension == ".zip":
            return ZipDataIngestor()
        else:
            raise ValueError(f"No ingestor available for file extension: {file_extension}")


def main():
    file_path = "../data/archive.zip"

    file_extension = os.path.splitext(file_path)[1]

    data_ingestor = DataIngestorFactory.get_data_ingestor(file_extension)

    df = data_ingestor.ingest(file_path)

    print(df.head()) 
    
# if __name__ == "__main__":
#     main()