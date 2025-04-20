import requests
import os
import logging
from include.utils import setup_logging
from include.config import path
from include.logger import get_logger

logger = get_logger(__name__)

def run():
    """
    Main function to fetch data from Mockaroo and save it to a CSV file.
    """
    import os
    print("🧠 Working directory:", os.getcwd())
    print("🔗 Log file path:", path("data", "logs", "extract.log"))
    print("🗃️ Raw data path:", path("data", "raw", "mockaroo_data.csv"))


    # Configure logging
    log_file_path = path("data", "logs", "extract.log")
    # project_dir = "/Users/radyhz/Documents/Projects/hfdb_project"
    # log_file_path = 'data/logs/extract.log'
    # print(os.path.join(project_dir, log_file_path))
    # setup_logging(log_file_path)
    
    print('logging was setup!')

    # Replace with your actual Mockaroo URL
    MOCKAROO_URL = "https://api.mockaroo.com/api/2f662d90?count=1000&key=98dee670"

    try:
        # Make the API request
        response = requests.get(MOCKAROO_URL)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

        # Define the correct path for raw data
        raw_data_path = path("data", "raw", "mockaroo_data.csv")
        # raw_data_path = os.path.join(project_dir, "data/raw/mockaroo_data.csv")

        # Ensure the directory exists
        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

        # Save the CSV data to a file
        with open(raw_data_path, "wb") as file:
            file.write(response.content)
        
        logger.info(f"✅ Data successfully saved to {raw_data_path}")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to fetch data from Mockaroo: {e}")
    except OSError as e:
        logger.error(f"❌ Failed to save data to {raw_data_path}: {e}")


if __name__ == "__main__":
    run()



