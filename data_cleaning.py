# class_3_data_cleaning.py

import pandas as pd
import numpy as np
import re
import json
import requests
from typing import List, Dict, Any


class DataCleaning:
    """
    Collection of data cleaning methods for DataFrames.
    """

    def __init__(self):
        """
        Initialize the DataCleaning class.
        """
        pass

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a series of cleaning steps for user data.

        Args:
            df (pd.DataFrame): The user data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = self.standardize_nulls(df)
        df = self.clean_address(df)
        df = self.clean_country_columns(df)
        df = self.clean_phone_number(df)
        df = self.clean_dates(df, date_columns=['date_of_birth', 'join_date'])
        df = self.remove_invalid_rows(df)
        return df

    def clean_card_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a series of cleaning steps for card details data.

        Args:
            df (pd.DataFrame): The card details DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = self.standardize_nulls(df)
        df = self.clean_dates(df, date_columns=['date_payment_confirmed'])
        df = self.remove_invalid_rows(df)
        df = self.clean_card_number(df)
        return df

    def clean_store_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a series of cleaning steps for store details.

        Args:
            df (pd.DataFrame): The store details DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = self.standardize_nulls(df)
        df = self.clean_address(df)
        df = self.merge_latitude_columns(df)
        df = self.clean_dates(df, date_columns=['opening_date'])
        df = self.clean_categorical_columns(df)
        df = self.clean_locality(df)
        df = self.clean_store_code(df)
        df = self.clean_staff_numbers(df)
        df = self.remove_invalid_rows_excluding_store_code(df)
        return df

    def clean_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a series of cleaning steps for product data.

        Args:
            df (pd.DataFrame): The product data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = self.standardize_nulls(df)
        df = self.remove_invalid_rows(df)
        df = self.clean_weight_column(df)
        df['product_price'] = df['product_price'].str.replace('£', '')
        df = self.convert_data_types(df, ['product_price'])
        df.rename(columns={'product_price': 'product_price_gbp'}, inplace=True)
        df = self.clean_dates(df, date_columns=['date_added'])
        return df

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a series of cleaning steps for orders data.

        Args:
            df (pd.DataFrame): The orders data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = self.standardize_nulls(df)
        df = self.remove_invalid_rows(df)
        columns_to_drop = ['first_name', 'last_name', '1']
        df = self.drop_columns(df, columns_to_drop)
        df = self.convert_data_types(df, ['product_quantity'])
        return df

    def clean_date_events_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Clean and transform JSON-based date events data into a DataFrame.

        Args:
            json_data (dict): The JSON data containing date events.

        Returns:
            pd.DataFrame: The cleaned and transformed DataFrame.
        """
        df = self.reformat_json_to_df(json_data)
        df = self.remove_null_rows(df)
        df = self.remove_invalid_rows_date_events_data(df)
        df = self.combine_datetime_columns(df)
        return df

    def standardize_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize null representations (NULL, None, N/A, '') to np.nan.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        null_representations = ['NULL', 'None', 'N/A', '']
        df.replace(null_representations, np.nan, inplace=True)
        df = df.where(pd.notnull(df), np.nan)
        return df

    def clean_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'address' column by replacing newline characters with commas.

        Args:
            df (pd.DataFrame): The DataFrame with an 'address' column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['address'] = df['address'].str.replace('\n', ',')
        return df

    def clean_country_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'country' and 'country_code' columns by removing invalid entries.

        Args:
            df (pd.DataFrame): The DataFrame with 'country' and 'country_code' columns.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['country'] = df['country'].apply(
            lambda x: x if not any(char.isdigit() for char in str(x)) else np.nan
        )
        df['country_code'] = df['country_code'].apply(
            lambda x: x if (not any(char.isdigit() for char in str(x)) and len(str(x)) <= 3) else np.nan
        )
        df['country_code'] = df['country_code'].replace('GGB', 'GB')
        return df

    def clean_phone_number(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'phone_number' column by removing non-digit characters.

        Args:
            df (pd.DataFrame): The DataFrame with a 'phone_number' column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(r'\D', '', str(x)))
        return df

    def clean_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """
        Convert specified columns to datetime, handling non-standard formats.

        Args:
            df (pd.DataFrame): The DataFrame to transform.
            date_columns (List[str]): List of columns to convert to datetime.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        for col in date_columns:
            df[col] = df[col].apply(
                lambda x: self.parse_non_standard_dates(x)
                if pd.isna(pd.to_datetime(x, errors='coerce'))
                else pd.to_datetime(x, errors='coerce')
            )
        return df

    def parse_non_standard_dates(self, date_str: str) -> pd.Timestamp:
        """
        Attempt to parse non-standard date strings into pd.Timestamp.

        Args:
            date_str (str): The date string to parse.

        Returns:
            pd.Timestamp or np.nan: A parsed Timestamp or NaN if it fails.
        """
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return pd.to_datetime(date_str, format='%Y-%m-%d')
            elif re.match(r'^\d{4}/\d{2}/\d{2}$', date_str):
                return pd.to_datetime(date_str, format='%Y/%m/%d')
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%d/%m/%Y')
            elif re.match(r'^\d{2}/\d{2}$', date_str):
                return pd.to_datetime(date_str, format='%m/%y')
            elif re.match(r'^\w+ \d{4} \d{2}$', date_str):
                return pd.to_datetime(date_str, format='%B %Y %d')
            elif re.match(r'^\d{4} \w+ \d{2}$', date_str):
                return pd.to_datetime(date_str, format='%Y %B %d')
            else:
                return pd.to_datetime(date_str, errors='coerce')
        except Exception:
            return np.nan

    def clean_card_number(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'card_number' column by removing invalid characters.

        Args:
            df (pd.DataFrame): The DataFrame with a 'card_number' column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['card_number'] = df['card_number'].apply(
            lambda x: re.sub(r'\?', '', str(x)) if isinstance(x, str) else x
        )
        return df

    def remove_invalid_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows containing invalid 10-character alphanumeric strings 
        that are not purely digits or purely letters.

        Args:
            df (pd.DataFrame): The DataFrame to filter.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        def is_invalid_pattern(val):
            if isinstance(val, str) and len(val) == 10 and val.isalnum() and not val.isdigit() and not val.isalpha():
                return True
            return False

        invalid_rows = df.apply(lambda row: any(is_invalid_pattern(val) for val in row), axis=1)
        df = df[~invalid_rows]
        return df

    def remove_invalid_rows_date_events_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows containing invalid 10-character alphanumeric strings 
        for date events data.

        Args:
            df (pd.DataFrame): The date events DataFrame.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        def is_invalid_pattern(val):
            if isinstance(val, str) and len(val) == 10 and val.isalnum() and not val.isdigit() and not val.isalpha():
                return True
            return False

        invalid_rows = df.apply(lambda row: any(is_invalid_pattern(val) for val in row), axis=1)
        df = df[~invalid_rows]
        return df

    def merge_latitude_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge columns 'lat' and 'latitude', prioritizing 'latitude' if both are present.

        Args:
            df (pd.DataFrame): The DataFrame containing 'lat' and 'latitude'.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['latitude'] = df['latitude'].combine_first(df['lat'])
        df.drop(columns=['lat'], inplace=True)
        return df

    def convert_data_types(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Convert specified columns to numeric types.

        Args:
            df (pd.DataFrame): The DataFrame containing the columns.
            columns (list): List of column names to convert to numeric.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        for column in columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        return df

    def clean_categorical_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean store_type, country_code, and continent columns by removing invalid entries.

        Args:
            df (pd.DataFrame): The DataFrame containing these categorical columns.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['store_type'] = df['store_type'].apply(
            lambda x: x if not any(char.isdigit() for char in str(x)) else np.nan
        )
        df['country_code'] = df['country_code'].apply(
            lambda x: x if (not any(char.isdigit() for char in str(x)) and len(str(x)) <= 3) else np.nan
        )
        df['continent'] = df['continent'].apply(
            lambda x: x.replace('ee', '') if isinstance(x, str) else x
        )
        df['continent'] = df['continent'].apply(
            lambda x: x if not any(char.isdigit() for char in str(x)) else np.nan
        )
        return df

    def clean_locality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'locality' column by ensuring it contains no digits.

        Args:
            df (pd.DataFrame): The DataFrame with a 'locality' column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['locality'] = df['locality'].apply(
            lambda x: x if (isinstance(x, str) and not any(char.isdigit() for char in x)) else np.nan
        )
        return df

    def clean_store_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function does not alter 'store_code' by default.

        Args:
            df (pd.DataFrame): The DataFrame with a 'store_code' column.

        Returns:
            pd.DataFrame: The original DataFrame.
        """
        return df

    def clean_staff_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the 'staff_numbers' column by removing all non-digit characters.

        Args:
            df (pd.DataFrame): The DataFrame with a 'staff_numbers' column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df['staff_numbers'] = df['staff_numbers'].apply(
            lambda x: re.sub(r'[^\d]', '', str(x)) if pd.notnull(x) else x
        )
        df['staff_numbers'] = df['staff_numbers'].replace('', np.nan)
        return df

    def clean_weight_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and convert the 'weight' column to kilograms.

        Args:
            df (pd.DataFrame): The DataFrame with a 'weight' column.

        Returns:
            pd.DataFrame: The transformed DataFrame with a new 'weight_kg' column.
        """
        def extract_number_unit(s):
            if pd.isna(s):
                return [None, None]
            parts = re.split(r'(\d+\.?\d*)', s.replace(' ', ''))
            if len(parts) < 3:
                return [None, None]
            number = parts[1]
            unit = ''.join(parts[2:]).lower().strip()
            return [number, unit]

        def normalize_units(unit):
            if not unit:
                return None
            unit = unit.lower().strip()
            if 'kg' in unit or 'kilogram' in unit:
                return 'kg'
            elif 'g' in unit or 'gram' in unit:
                return 'g'
            elif 'ml' in unit or 'milliliter' in unit or 'millilitre' in unit:
                return 'g'
            elif 'liter' in unit or 'liters' in unit or 'litre' in unit or 'litres' in unit:
                return 'liters'
            else:
                return None

        def convert_to_kilograms(row):
            try:
                number = float(row['number'])
                unit = row['unit']
                if unit == 'g':
                    return number / 1000
                elif unit == 'kg':
                    return number
                elif unit == 'liters':
                    return number
                else:
                    return None
            except (TypeError, ValueError):
                return None

        df[['number', 'unit']] = df['weight'].apply(extract_number_unit).apply(pd.Series)
        df['unit'] = df['unit'].apply(normalize_units)
        df['weight_kg'] = df.apply(convert_to_kilograms, axis=1)
        df.drop(['weight', 'number', 'unit'], axis=1, inplace=True)
        return df

    def drop_columns(self, df: pd.DataFrame, columns_to_drop: List[str]) -> pd.DataFrame:
        """
        Drop the specified columns from the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame from which to drop columns.
            columns_to_drop (List[str]): List of column names to drop.

        Returns:
            pd.DataFrame: The DataFrame without the specified columns.
        """
        df = df.drop(columns=columns_to_drop, errors='ignore')
        return df

    def fetch_and_save_json(self, url: str, filename: str) -> Dict[str, Any]:
        """
        Fetch the JSON data from a given URL and save it to a local file.

        Args:
            url (str): The URL from which to fetch JSON data.
            filename (str): The name of the file where data will be saved.

        Returns:
            dict: The JSON data retrieved from the URL.
        """
        response = requests.get(url)
        raw_json_data = response.json()
        with open(filename, 'w') as f:
            json.dump(raw_json_data, f, indent=4)
        return raw_json_data

    def reformat_json_to_df(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Reformat a specific JSON structure into a pandas DataFrame.

        Args:
            json_data (dict): The JSON data containing 'timestamp', 'month', 'year', etc.

        Returns:
            pd.DataFrame: A DataFrame containing the reformatted JSON data.
        """
        records = []
        for key in json_data['timestamp'].keys():
            record = {
                'timestamp': json_data['timestamp'][key],
                'month': json_data['month'][key],
                'year': json_data['year'][key],
                'day': json_data['day'][key],
                'time_period': json_data['time_period'][key],
                'date_uuid': json_data['date_uuid'][key]
            }
            records.append(record)
        return pd.DataFrame(records)

    def remove_null_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows with null values in crucial columns (timestamp, day, month, year).

        Args:
            df (pd.DataFrame): The DataFrame to filter.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        df.replace('NULL', pd.NA, inplace=True)
        df.dropna(subset=['timestamp', 'day', 'month', 'year'], inplace=True)
        return df

    def combine_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine 'year', 'month', 'day', and 'timestamp' into a single 'datetime' column,
        then drop the original columns.

        Args:
            df (pd.DataFrame): The DataFrame containing the date/time columns.

        Returns:
            pd.DataFrame: The DataFrame with the new 'datetime' column.
        """
        df['datetime'] = pd.to_datetime(
            df['year'].astype(str)
            + '-'
            + df['month'].astype(str)
            + '-'
            + df['day'].astype(str)
            + ' '
            + df['timestamp']
        )
        df.drop(['timestamp', 'day', 'month', 'year'], axis=1, inplace=True)
        return df

    def remove_invalid_rows_excluding_store_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows with NaN in 'store_code', except preserve a specific store_code.

        Args:
            df (pd.DataFrame): The DataFrame containing the 'store_code' column.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        preserved_row_condition = df['store_code'] == 'WEB-1388012W'
        df = df[~df['store_code'].isna() | preserved_row_condition]
        return df