# class_2_database_connector.py

import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from typing import Optional, Dict


class DatabaseConnector:
    """
    Connector class for interacting with a Postgres database via SQLAlchemy.
    """

    def __init__(self, config_path: str = "db_creds.yaml"):
        """
        Initialize the DatabaseConnector with a path to the YAML file containing DB credentials.

        Args:
            config_path (str, optional):
                Path to the YAML file that contains database credentials.
                Defaults to "db_creds.yaml".
        """
        self.config = self._read_db_creds(config_path)
        self.engine = self._init_db_engine()

    def _read_db_creds(self, path: str) -> Optional[Dict[str, str]]:
        """
        Read database credentials from a YAML file.

        Args:
            path (str): The path to the YAML file.

        Returns:
            dict or None: A dictionary of DB credentials or None if any error occurs.
        """
        try:
            with open(path, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except FileNotFoundError:
            print(f"Error: The file {path} was not found.")
            return None
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML file: {exc}")
            return None

    def _init_db_engine(self) -> Optional[create_engine]:
        """
        Initialize a SQLAlchemy engine based on the credentials.

        Returns:
            create_engine or None: A SQLAlchemy engine if successful, otherwise None.
        """
        if not self.config:
            print("No configuration available.")
            return None

        try:
            db_url = (
                f"postgresql://{self.config['RDS_USER']}:{self.config['RDS_PASSWORD']}"
                f"@{self.config['RDS_HOST']}:{self.config['RDS_PORT']}/{self.config['RDS_DATABASE']}"
            )
            engine = create_engine(db_url)
            return engine
        except KeyError as e:
            print(f"Missing key in database credentials: {e}")
            return None
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None

    def list_db_tables(self) -> Optional[list]:
        """
        List all tables in the database.

        Returns:
            list or None: A list of table names if successful, otherwise None.
        """
        if not self.engine:
            print("Database engine is not initialized.")
            return None

        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except Exception as e:
            print(f"Error listing tables: {e}")
            return None

    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        """
        Upload a pandas DataFrame to the specified table in the database.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the table to upload to.
        """
        if not self.engine:
            print("Database engine is not initialized.")
            return

        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data uploaded to table {table_name} successfully.")
        except Exception as e:
            print(f"Error uploading data to table {table_name}: {e}")

    def reformat_json_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Example placeholder method for reformatting JSON stored in a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame that might contain JSON data.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        # Placeholder logic
        json_data = df.iloc[0]
        if 'timestamp' not in json_data:
            raise KeyError("'timestamp' key not found in JSON data")

        # Example iteration
        for key in json_data['timestamp'].keys():
            pass

        return df
