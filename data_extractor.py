# class_1_data_extractor.py

import configparser
import yaml
import pandas as pd
import requests
import tabula
import boto3
from sqlalchemy import text
from io import StringIO

from database_connector import DatabaseConnector


class DataExtractor:
    """
    Extractor class for reading data from multiple sources such as RDS databases,
    PDF files, APIs, and S3 buckets.
    """

    def __init__(
        self,
        db_connector: DatabaseConnector = None,
        config_path_ini: str = "config.ini",
        config_path_yaml: str = "api_conn.yaml"
    ):
        """
        Initialize the DataExtractor instance.

        Args:
            db_connector (DatabaseConnector, optional):
                An instance of DatabaseConnector for database operations.
                Defaults to None.
            config_path_ini (str, optional):
                Path to the config.ini file containing API endpoints and S3 URIs.
                Defaults to "config.ini".
            config_path_yaml (str, optional):
                Path to the YAML file containing the API key. Defaults to "api_conn.yaml".
        """
        self.db_connector = db_connector

        # Load config.ini
        self.config = configparser.ConfigParser()
        self.config.read(config_path_ini)

        # Load API key from api_conn.yaml
        with open(config_path_yaml, "r") as f:
            api_config = yaml.safe_load(f)
        self.api_key = api_config.get("api_key", "")
        self.headers = {"x-api-key": self.api_key}

        # Extract endpoints from config.ini
        self.stores_endpoint = self.config["API"].get("stores_endpoint", "")
        self.store_details_endpoint = self.config["API"].get("store_details_endpoint", "")
        self.pdf_link = self.config["API"].get("pdf_link", "")
        self.s3_uri = self.config["API"].get("s3_uri", "")

    def list_tables(self) -> list:
        """
        List all tables in the database using the provided DatabaseConnector.

        Returns:
            list: A list of table names, or an empty list if the connection is missing.
        """
        if self.db_connector:
            return self.db_connector.list_db_tables()
        else:
            print("No database connection provided.")
            return []

    def read_data(self, table_name: str) -> list:
        """
        Read data from the specified table and return it as a list of dictionaries.

        Args:
            table_name (str): Name of the table to read.

        Returns:
            list: A list of dictionaries representing the rows of the table,
                  or an empty list if an error occurs or if the connection is missing.
        """
        if self.db_connector:
            try:
                with self.db_connector.engine.connect() as connection:
                    query = text(f"SELECT * FROM {table_name}")
                    result = connection.execute(query)
                    return [dict(row) for row in result.mappings()]
            except Exception as e:
                print(f"Error reading data from table {table_name}: {e}")
                return []
        else:
            print("No database connection provided.")
            return []

    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        """
        Read a table from the RDS database into a pandas DataFrame.

        Args:
            table_name (str): Name of the table to read.

        Returns:
            pd.DataFrame: A DataFrame of the table data,
                          or an empty DataFrame if an error occurs or the connection is missing.
        """
        if self.db_connector:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, self.db_connector.engine)
                return df
            except Exception as e:
                print(f"Error reading table {table_name}: {e}")
                return pd.DataFrame()
        else:
            print("No database connection provided.")
            return pd.DataFrame()

    def retrieve_pdf_data(self) -> pd.DataFrame:
        """
        Retrieve data from a PDF file whose link is specified in config.ini.

        Returns:
            pd.DataFrame: A DataFrame of concatenated tables extracted from the PDF,
                          or an empty DataFrame if an error occurs.
        """
        try:
            if not self.pdf_link:
                print("PDF link not found in config.ini.")
                return pd.DataFrame()

            df_list = tabula.read_pdf(self.pdf_link, pages="all", multiple_tables=True)
            df = pd.concat(df_list, ignore_index=True)
            return df
        except Exception as e:
            print(f"Error retrieving data from PDF: {e}")
            return pd.DataFrame()

    def list_number_of_stores(self) -> int:
        """
        Retrieve the number of stores from the API endpoint specified in config.ini.

        Returns:
            int: The number of stores, or 0 if an error occurs or the endpoint is missing.
        """
        try:
            if not self.stores_endpoint:
                print("Stores endpoint not found in config.ini.")
                return 0

            response = requests.get(self.stores_endpoint, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data.get("number_stores", 0)
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving number of stores: {e}")
            return 0

    def retrieve_stores_data(self, number_of_stores: int) -> pd.DataFrame:
        """
        Retrieve the details of each store from the store_details_endpoint in config.ini.

        Args:
            number_of_stores (int): The number of stores to retrieve.

        Returns:
            pd.DataFrame: A DataFrame containing store details,
                          or an empty DataFrame if the endpoint is missing.
        """
        if not self.store_details_endpoint:
            print("Store details endpoint not found in config.ini.")
            return pd.DataFrame()

        stores_data = []
        for store_number in range(number_of_stores):
            try:
                url = f"{self.store_details_endpoint}/{store_number}"
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                stores_data.append(response.json())
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving data for store number {store_number}: {e}")

        return pd.DataFrame(stores_data)

    def extract_from_s3(self) -> pd.DataFrame:
        """
        Extract data from an S3 CSV file. The S3 URI is read from config.ini.

        Returns:
            pd.DataFrame: A DataFrame of the CSV contents,
                          or an empty DataFrame if an error occurs or the URI is missing.
        """
        if not self.s3_uri:
            print("S3 URI not found in config.ini.")
            return pd.DataFrame()

        bucket_name, s3_file_key = self._parse_s3_uri(self.s3_uri)
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
            csv_string = response["Body"].read().decode("utf-8")
            return pd.read_csv(StringIO(csv_string))
        except boto3.exceptions.Boto3Error as e:
            print(f"Error extracting data from S3: {e}")
            return pd.DataFrame()

    def extract_json_from_url(self, url: str) -> pd.DataFrame:
        """
        Extract JSON data from a given URL.

        Args:
            url (str): The URL pointing to JSON data.

        Returns:
            pd.DataFrame: A DataFrame with the normalized JSON data,
                          or an empty DataFrame if an error occurs.
        """
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            json_data = response.json()
            return pd.json_normalize(json_data)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from URL: {e}")
            return pd.DataFrame()

    @staticmethod
    def _parse_s3_uri(s3_uri: str) -> tuple:
        """
        Parse the S3 URI into bucket name and file key.

        Args:
            s3_uri (str): The S3 URI in the format 's3://bucket-name/path/to/file.csv'.

        Returns:
            tuple: A tuple (bucket_name, s3_file_key).
        """
        bucket_name = s3_uri.split("/")[2]
        s3_file_key = "/".join(s3_uri.split("/")[3:])
        return bucket_name, s3_file_key
