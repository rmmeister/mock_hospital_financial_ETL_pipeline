import psycopg2
import pandas as pd
import logging
import os
from include.config import path
from include.utils import setup_logging, read_data
from include.logger import get_logger

logger = get_logger(__name__)

def run():
    """Main function to insert data into PostgreSQL tables."""
    # Setup logging
    # setup_logging(path('data', 'logs', 'load.log'))

    # Load the processed data
    input_path = path("data", "processed", "processed_data.csv")
    df = read_data(input_path)

    # Split the data into relevant dataframes
    transactions = df[["transaction_id", "fiscal_year", "submission_date", "org_id",
                       "accounting_centre", "financial_account", "amount"]]

    facilities = df[["org_id", "org_province", "org_city", "org_name",
                     "org_type", "org_zip"]].drop_duplicates('org_id')

    # Connect to PostgreSQL database
    conn = None
    try:
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(
            dbname="hfdb",
            user="root",
            password="1234",
            host="host.docker.internal",
            port="5433"
        )
        cursor = conn.cursor()

        logger.info("Starting database insertion...")

        # Insert into FACILITIES table
        for _, row in facilities.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO FACILITIES (
                        FACILITY_ID,
                        FACILITY_STATE,
                        FACILITY_CITY,
                        FACILITY_NAME,
                        FACILITY_TYPE,
                        POSTAL_CODE
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (FACILITY_ID) DO UPDATE SET 
                        FACILITY_STATE = EXCLUDED.FACILITY_STATE,
                        FACILITY_CITY = EXCLUDED.FACILITY_CITY,
                        FACILITY_NAME = EXCLUDED.FACILITY_NAME,
                        FACILITY_TYPE = EXCLUDED.FACILITY_TYPE,
                        POSTAL_CODE = EXCLUDED.POSTAL_CODE;
                    """,
                    tuple(row)
                )
            except Exception as e:
                logger.error(f"Error inserting facility {row['FACILITY_ID']}: {e}")
                conn.rollback()

        # Insert into TRANSACTIONS table
        for _, row in transactions.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO TRANSACTIONS (
                        TRANSACTION_ID,
                        FISCAL_YEAR,
                        SUBMISSION_DATE,
                        FACILITY_ID,
                        ACCOUNTING_CENTRE,
                        FINANCIAL_ACCOUNT,
                        AMOUNT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (TRANSACTION_ID) DO UPDATE SET 
                        FISCAL_YEAR = EXCLUDED.FISCAL_YEAR,
                        SUBMISSION_DATE = EXCLUDED.SUBMISSION_DATE,
                        FACILITY_ID = EXCLUDED.FACILITY_ID,
                        ACCOUNTING_CENTRE = EXCLUDED.ACCOUNTING_CENTRE,
                        FINANCIAL_ACCOUNT = EXCLUDED.FINANCIAL_ACCOUNT,
                        AMOUNT = EXCLUDED.AMOUNT;
                    """,
                    tuple(row)
                )
            except Exception as e:
                logger.error(f"Error inserting transaction {row['TRANSACTION_ID']}: {e}")
                conn.rollback()

        conn.commit()
        logger.info("Database insertion completed successfully.")

    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

# Run the function
if __name__ == "__main__":
    run()