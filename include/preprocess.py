import pandas as pd
import os
import sys
import logging
import datetime
import numpy as np
from include.config import path
from include.utils import setup_logging, read_data
from include.logger import get_logger

logger = get_logger(__name__)

def run():
    """
    Main function to execute the preprocessing script with exception handling.
    """
    try:
        # Initialize paths and logging
        input_path, processed_data_path = setup_paths_and_logging()

        # Read the input data
        df = read_data(input_path)

        # Perform data quality checks
        perform_data_quality_checks(df, input_path)

        # Clean and transform the data
        df = clean_and_transform_data(df)

        # Validate the schema of the DataFrame
        validate_schema(df)

        # Save the processed data to a new CSV file
        output_path = os.path.join(processed_data_path, "processed_data.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Processed data saved to {output_path}")

    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
    except pd.errors.EmptyDataError as e:
        logger.error(f"❌ Empty data error: {e}")
    except ValueError as e:
        logger.error(f"❌ Value error: {e}")
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}")


def setup_paths_and_logging():
    """
    Set up directory paths and logging for the preprocessing script.
    """
    # Set up directory paths
    base_data_path = "data"
    raw_data_path = path(base_data_path, "raw")
    processed_data_path = path(base_data_path, "processed")
    logs_path = path(base_data_path, "logs")

    # Create necessary directories
    os.makedirs(raw_data_path, exist_ok=True)
    os.makedirs(processed_data_path, exist_ok=True)
    os.makedirs(logs_path, exist_ok=True)

    # Set up logging
    log_file_path = os.path.join(logs_path, "preprocess.log")
    # setup_logging(log_file_path)

    # Define input file path
    input_path = os.path.join(raw_data_path, "mockaroo_data.csv")
    return input_path, processed_data_path


def perform_data_quality_checks(df, input_path):
    """
    Perform data quality checks on the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        input_path (str): Path to the input CSV file.

    Returns:
        None
    """
    # Check if the CSV file exists
    if not os.path.exists(input_path):
        logger.error("❌ CSV file not found.")
        return

    # Check if the DataFrame is empty
    if df.empty:
        logger.error("❌ No data found in the CSV file.")
        return

    # Check if the DataFrame has the expected columns
    expected_columns = ["org_name", "financial_account", "amount", "accounting_centre"]
    for column in expected_columns:
        if column not in df.columns:
            logger.error(f"❌ Missing expected column: {column}")

    # Check for duplicate rows
    duplicate_rows = df[df.duplicated()]
    if not duplicate_rows.empty:
        logger.warning(f"⚠️ Found {len(duplicate_rows)} duplicate rows in the DataFrame.")
    else:
        logger.info("✅ No duplicate rows found.")

    # Check for missing values
    missing_values = df.isnull().sum()
    if missing_values.any():
        logger.warning("⚠️ Missing values found in the DataFrame:")
        logger.debug(missing_values[missing_values > 0])
    else:
        logger.info("✅ No missing values found.")

    # Check for outliers in the 'amount' column
    amount_mean = df['amount'].mean()
    amount_std = df['amount'].std()
    outlier_threshold = 3
    outliers = df[
        (df['amount'] < amount_mean - outlier_threshold * amount_std) | 
        (df['amount'] > amount_mean + outlier_threshold * amount_std)
    ]
    if not outliers.empty:
        logger.warning(f"⚠️ Found {len(outliers)} outliers in the 'amount' column.")
    else:
        logger.info("✅ No outliers found in the 'amount' column.")

    # Check for duplicate IDs in specific columns
    def check_duplicate_ids(df, col):
        if df[col].duplicated().any():
            logger.warning(f"⚠️ Duplicate IDs found in the '{col}' column.")
        else:
            logger.info(f"✅ No duplicate IDs found in the '{col}' column.")

    check_duplicate_ids(df, 'org_id')
    check_duplicate_ids(df, 'transaction_id')


def clean_and_transform_data(df):
    """
    Clean and transform the DataFrame by performing various preprocessing steps.

    Args:
        df (pd.DataFrame): The DataFrame to clean and transform.

    Returns:
        pd.DataFrame: The cleaned and transformed DataFrame.
    """
    # Drop unnecessary columns
    if 'rand' in df.columns:
        df.drop(columns='rand', inplace=True)

    # Drop rows with missing values in the 'amount' column
    df.dropna(subset=['amount'], inplace=True)

    # Handle missing values in the 'org_type' column by replacing them with 'UNKNOWN'
    if df['org_type'].dtype.name == 'category':
        df['org_type'] = df['org_type'].cat.add_categories('UNKNOWN').fillna('UNKNOWN')

        # Handle inconsistent data in the 'org_type' column when it is of type category
        hospital_mask = ~df['org_type'].str.contains('non', case=False, na=False) & (df['org_type'] != 'UNKNOWN')
        non_hospital_mask = df['org_type'].str.contains('non', case=False, na=False)
        # Ensure the categories 'HOSPITAL' and 'NON-HOSPITAL' are added if not already present
        if 'HOSPITAL' not in df['org_type'].cat.categories:
            df['org_type'] = df['org_type'].cat.add_categories('HOSPITAL')
        if 'NON-HOSPITAL' not in df['org_type'].cat.categories:
            df['org_type'] = df['org_type'].cat.add_categories('NON-HOSPITAL')

        df.loc[hospital_mask, 'org_type'] = 'HOSPITAL'
        df.loc[non_hospital_mask, 'org_type'] = 'NON-HOSPITAL'
        df['org_type'] = df['org_type'].cat.remove_unused_categories()
        logger.info(f"✅ Replaced inconsistent values in 'org_type' column: {df['org_type'].unique()}")
    else:
        df['org_type'].fillna('UNKNOWN', inplace=True)
        hospital_mask = ~df['org_type'].str.contains('non', case=False, na=False) & (df['org_type'] != 'UNKNOWN')
        non_hospital_mask = df['org_type'].str.contains('non', case=False, na=False)
        df.loc[hospital_mask, 'org_type'] = 'HOSPITAL'
        df.loc[non_hospital_mask, 'org_type'] = 'NON-HOSPITAL'

        logger.info(f"✅ Replaced inconsistent values in 'org_type' column: {df['org_type'].unique()}")

    # Convert submission_date to fiscal year
    def convert_to_fiscal_year(date_str):
        date = pd.to_datetime(date_str, format='%m/%d/%Y', errors='coerce')
        if pd.isnull(date):
            logger.warning(f"⚠️ Invalid date format: {date_str}")
            return None
        fiscal_year = date.year if date.month >= 4 else date.year - 1
        return fiscal_year

    df['fiscal_year'] = df['submission_date'].apply(convert_to_fiscal_year)
    df['fiscal_year'] = df['fiscal_year'].astype(int)
    df['submission_date'] = pd.to_datetime(df['submission_date'], format='%m/%d/%Y', errors='coerce')

    # Bring the 'fiscal_year' column to the front
    fiscal_year_col = df.pop('fiscal_year')
    df.insert(0, 'fiscal_year', fiscal_year_col)

    return df


def validate_schema(df: pd.DataFrame) -> None:
    """
    Validate the schema of the DataFrame by checking column names and data types.

    Args:
        df (pd.DataFrame): The DataFrame to validate.

    Returns:
        None
    """
    # Expected schema: column names and data types
    expected_schema = {
        "transaction_id": int,
        "fiscal_year": int,
        "submission_date": "datetime64[ns]",
        "org_province": str,
        "org_name": str,
        "org_type": str,
        "accounting_centre": str,
        "financial_account": str,
        "amount": float
    }

    # Validate data types
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            logger.error(f"❌ Missing column: {col}")
        elif not pd.api.types.is_dtype_equal(df[col].dtype, expected_type):
            logger.warning(f"⚠️ Column '{col}' has incorrect type {df[col].dtype}, expected {expected_type}")


if __name__ == "__main__":
    run()