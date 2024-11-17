import os
import zipfile
from abc import ABC, abstractmethod

import pandas as pd

# Define an abstract class for Data Ingestion
class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Abstract method to ingest data from a given file."""
        pass


# Implement a concrete class for ZIP Ingestion
class ZipDataIngestor(DataIngestor):
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Extracts a .zip file and returns the content as a pandas DataFrame."""
        
        # Check if the file is a .zip file
        if not file_path.endswith(".zip"):
            raise ValueError("The provided file is not a .zip file.")

        # Extract the contents of the .zip file
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall("../extracted_data")

        # Check if the extracted data contains a CSV file
        extracted_files = os.listdir("../extracted_data")
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if len(csv_files) == 0:
            raise FileNotFoundError("No CSV files found in the extracted data.")
        if len(csv_files) > 1:
            raise ValueError("Multiple CSV files found in the extracted data.")

        # Load the CSV file into a DataFrame
        csv_file_path = os.path.join("../extracted_data", csv_files[0])
        df = pd.read_csv(csv_file_path)

        # Return the DataFrame
        return df


# Implement a Factory to create DataIngestors
class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(file_extension: str) -> DataIngestor:
        """Returns the appropriate DataIngestor based on file extension."""
        
        if file_extension == ".zip":
            return ZipDataIngestor()
        else:
            raise ValueError(f"No ingestor available for file extension: {file_extension}")


# Test
if __name__ == "__main__":
    file_path = "../data/archive.zip"

    file_extension = os.path.splitext(file_path)[1]

    data_ingestor = DataIngestorFactory.get_data_ingestor(file_extension)

    df = data_ingestor.ingest(file_path)

    print(df.head()) 