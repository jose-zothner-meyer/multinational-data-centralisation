import configparser
import yaml  # <-- We use PyYAML to load from .yaml
import re
import requests
import pandas as pd

from database_connector import DatabaseConnector
from data_extractor import DataExtractor
from data_cleaning import DataCleaning
import pkg_resources

# 1. Load config.ini
config = configparser.ConfigParser()
config.read("config.ini")  # Update path if needed

# 2. Load the API key from api_conn.yaml
with open('api_conn.yaml', 'r') as f:
    api_config = yaml.safe_load(f)

# Extract endpoints from config.ini
STORES_ENDPOINT = config["API"]["stores_endpoint"]
STORE_DETAILS_ENDPOINT = config["API"]["store_details_endpoint"]
PDF_LINK = config["API"]["pdf_link"]
S3_URI = config["API"]["s3_uri"]
JSON_URL = config["API"]["json_url"]

# Extract the API key from the YAML file
API_KEY = api_config['api_key']
HEADERS = {"x-api-key": API_KEY}


def users_clean():
    """
    Cleans the user data from the AWS RDS database and uploads it 
    into a local database as 'dim_users'.
    """
    rds_db_connector = DatabaseConnector(config_path='aws_db_creds.yaml')
    data_extractor = DataExtractor(rds_db_connector)
    tables = data_extractor.list_tables()

    target_table = 'legacy_users'

    if target_table in tables:
        df = data_extractor.read_rds_table(target_table)
        if df is not None:
            print("Data before cleaning:")
            print(df.head())

            data_cleaner = DataCleaning()
            cleaned_df = data_cleaner.clean_user_data(df)
            print("Data after cleaning:")
            print(cleaned_df.head())

            local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
            local_db_connector.upload_to_db(cleaned_df, "dim_users")
    else:
        print(f"Table {target_table} not found in the database.")


def card_details_clean():
    """
    Cleans the card details data from a PDF file and uploads it 
    into a local database as 'dim_card_details'.
    """
    data_extractor = DataExtractor()
    pdf_data_df = data_extractor.retrieve_pdf_data()  # No PDF_LINK argument

    if pdf_data_df is not None:
        print("Data before cleaning:")
        print(pdf_data_df.head())

        data_cleaner = DataCleaning()
        cleaned_df = data_cleaner.standardize_nulls(pdf_data_df)
        cleaned_df = data_cleaner.clean_card_number(cleaned_df)
        cleaned_df = data_cleaner.clean_dates(cleaned_df, date_columns=['date_payment_confirmed'])
        cleaned_df = data_cleaner.remove_invalid_rows(cleaned_df)

        print("Final cleaned data:")
        print(cleaned_df.head())

        local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
        local_db_connector.upload_to_db(cleaned_df, "dim_card_details")
    else:
        print("Failed to retrieve data from the PDF.")


def stores_clean():
    """
    Cleans store details retrieved via API endpoints and uploads it 
    into a local database as 'dim_store_details'.
    """
    data_extractor = DataExtractor()

    number_of_stores = data_extractor.list_number_of_stores()
    print(f"Number of stores: {number_of_stores}")

    if number_of_stores:
        stores_df = data_extractor.retrieve_stores_data(number_of_stores)
        print("Data before cleaning:")
        print(stores_df.head())

        data_cleaner = DataCleaning()
        cleaned_df = data_cleaner.clean_store_details(stores_df)
        cleaned_df = cleaned_df.applymap(lambda x: re.sub(r',\s*', ', ', x) if isinstance(x, str) else x)

        print("Data after cleaning:")
        print(cleaned_df.head())

        local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
        local_db_connector.upload_to_db(cleaned_df, "dim_store_details")
    else:
        print("Failed to retrieve number of stores.")


def product_clean():
    """
    Cleans the product data retrieved from an S3 CSV file 
    and uploads it into a local database as 'dim_products'.
    """
    data_extractor = DataExtractor()
    products_df = data_extractor.extract_from_s3()
    print("Data before cleaning:")
    print(products_df.head())

    data_cleaner = DataCleaning()
    cleaned_df = data_cleaner.clean_product_data(products_df)

    print("Data after cleaning:")
    print(cleaned_df.head())

    local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
    local_db_connector.upload_to_db(cleaned_df, "dim_products")


def orders_clean():
    """
    Cleans the orders data from the AWS RDS database and uploads it 
    into a local database as 'orders_table'.
    """
    rds_db_connector = DatabaseConnector(config_path='aws_db_creds.yaml')
    data_extractor = DataExtractor(rds_db_connector)
    orders_df = data_extractor.read_rds_table('orders_table')
    print("Data before cleaning:")
    print(orders_df.head())

    data_cleaner = DataCleaning()
    cleaned_df = data_cleaner.clean_orders_data(orders_df)
    print("Data after cleaning:")
    print(cleaned_df.head())

    local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
    local_db_connector.upload_to_db(cleaned_df, "orders_table")


def dates_clean():
    """
    Cleans the dates data retrieved from a JSON file (fetched from S3) 
    and uploads it into a local database as 'dim_date_times'.
    """
    data_cleaner = DataCleaning()
    raw_json_data = data_cleaner.fetch_and_save_json(JSON_URL, "date_details.json")

    if raw_json_data:
        cleaned_df = data_cleaner.clean_date_events_data(raw_json_data)
        if cleaned_df is not None:
            print("Final cleaned data:")
            print(cleaned_df.head())

            local_db_connector = DatabaseConnector(config_path='local_db_creds.yaml')
            local_db_connector.upload_to_db(cleaned_df, "dim_date_times")


if __name__ == "__main__":
    # Example usage:
    users_clean()
    card_details_clean()
    stores_clean()
    product_clean()
    orders_clean()
    dates_clean()

    def export_requirements():
        with open('requirements.txt', 'w') as f:
            for dist in pkg_resources.working_set:
                f.write(f"{dist.project_name}=={dist.version}\n")

    export_requirements()