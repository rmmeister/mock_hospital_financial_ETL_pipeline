import logging
import os
from include.logger import get_logger
logger = get_logger(__name__)

def setup_logging(logfile='logs/app.log', level=logging.INFO):
    """
    Sets up logging to both a file and the console.

    :param logfile: Path to the log file.
    :param level: Logging level (default: logging.INFO).
    """
    logger = logging.getLogger()
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Ensure the log directory exists
    log_dir = os.path.dirname(logfile)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # File handler
    file_handler = logging.FileHandler(logfile)
    file_handler.setLevel(level)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(file_format)
    logger.addHandler(console_handler)


def read_data(file_path):
    """|
    Reads data from a file based on its type and optimizes object columns by converting them to category type
    if they have a limited number of unique values.

    Args:
        file_path (str): Path to the data file.

    Returns:
        DataFrame: Data read from the file.
    """
    import pandas as pd
    import os

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    elif file_path.endswith('.json'):
        df = pd.read_json(file_path)
    else:
        logger.error(f"Unsupported file type: {file_path}")
        return None

    # Optimize object columns by converting to category type
    for col in df.select_dtypes(include=['object']).columns:
        num_unique_values = df[col].nunique()
        num_total_values = len(df[col])
        if num_unique_values / num_total_values < 0.1:  # Threshold for conversion
            df[col] = df[col].astype('category')

    # Log number of rows and columns
    logger.info(f"Data loaded: {file_path} with shape {df.shape}")
    
    return df