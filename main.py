import os
import logging
import requests
import zipfile
import pandas as pd
from datetime import datetime
from time import sleep
import platform

# Set up logging
logging.basicConfig(filename='data_ingestion.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL for the dataset (make sure you have the correct download link from Kaggle)
#DATASET_URL = "https://www.kaggle.com/datasets/mehmetsabrikunt/internet-service-churn/download"
DATASET_URL = "https://github.com/sachin-laddha/DMML-Group-46/blob/main/internet_service_churn.csv"

# Function to get the current OS and construct paths accordingly
def get_storage_path(base_path, dataset_name, data_type):
    current_os = platform.system().lower()
    if current_os == "windows":
        base_path = base_path.replace("/", "\\")  # Handle Windows-style paths
    elif current_os == "linux" or current_os == "darwin":
        base_path = base_path.replace("\\", "/")  # Handle Linux/Mac paths
    # Create the directory path
    timestamp = datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join(base_path, dataset_name, data_type, timestamp)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Create directories if not exist
    return folder_path


# Function to download and unzip the dataset
def download_and_extract_data(storage_path):
    try:
        logging.info('Starting data download...')

        # Make a GET request to download the dataset as a zip file
        response = requests.get(DATASET_URL, timeout=10)

        # If the request is successful (status code 200)
        if response.status_code == 200:
            zip_file_path = os.path.join(storage_path, "internet_service_churn.zip")

            # Save the zip file
            with open(zip_file_path, 'wb') as file:
                file.write(response.content)

            logging.info(f'Data zip file successfully downloaded and saved as {zip_file_path}')

            # Extract the zip file to get the CSV
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(storage_path)
                logging.info(f"Extracted files: {zip_ref.namelist()}")

                # Assuming there's only one CSV in the zip file, get the first CSV file
                csv_file = zip_ref.namelist()[0]
                csv_file_path = os.path.join(storage_path, csv_file)

                # Load the dataset into pandas DataFrame for further inspection
                df = pd.read_csv(csv_file_path)
                logging.info(f"Dataset loaded successfully. Shape: {df.shape}")

                # Optionally, save the CSV file to a more readable name
                os.rename(csv_file_path, os.path.join(storage_path, "internet_service_churn.csv"))
                logging.info("Dataset renamed to 'internet_service_churn.csv'")

            return True  # Indicating successful download and extraction

        else:
            logging.error(f"Failed to download dataset. Status Code: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred during download: {str(e)}")
        return False
    except zipfile.BadZipFile as e:
        logging.error(f"Error with the downloaded zip file: {str(e)}")
        return False


# Function to schedule daily downloads (using sleep for simulation)
def schedule_daily_download(storage_path):
    while True:
        logging.info('Running daily data download...')
        success = download_and_extract_data(storage_path)

        if success:
            logging.info("Data download successful. Waiting for the next scheduled download.")
        else:
            logging.error("Data download failed. Retrying after 1 hour.")

        # Wait for 24 hours (86400 seconds) before the next download attempt
        sleep(86400)  # 24 hours


if __name__ == "__main__":
    # Define the base path (storage location input)
    storage_base_path = input("Enter the storage path (e.g., /path/to/storage/): ")
    dataset_name = "internet_service_churn"  # Dataset name
    data_type = "raw_data"  # Type of data (raw or transformed)

    # Get the correct storage path
    storage_path = get_storage_path(storage_base_path, dataset_name, data_type)

    # Start the data ingestion job
    logging.info("Data ingestion job started.")
    schedule_daily_download(storage_path)