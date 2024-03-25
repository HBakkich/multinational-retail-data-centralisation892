import pandas as pd  # Importing pandas library for data manipulation
pd.options.mode.chained_assignment = None  # Disabling chained assignment warnings
import data_extraction  # Importing custom module for data extraction
import database_utils  # Importing custom module for database utilities
from quantiphy import Quantity  # Importing Quantity class for unit conversion
import yaml



# Header and endpoint URLs for API requests
with open('db_creds.yaml', 'r') as stream:
    header = {'x-api-key': yaml.safe_load(stream)['X-API-KEY']} # api key value stored in yaml credentials file
retrieve_a_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
number_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

class DataCleaning():
    def __init__(self) -> None:
        pass  # No initialization logic present

    def clean_user_data(self):
        # Method to clean and upload user data to the database
        df = data_extraction.DataExtractor().read_rds_table('legacy_users')  # Reading user data from RDS database
        # Cleaning date columns, phone numbers, and country codes
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', dayfirst=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', dayfirst=True, errors='coerce')
        df['phone_number'] = df['phone_number'].str.replace(r'^.*\)', '', regex=True).replace(r'\D+', '', regex=True).str.lstrip('0')
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        for i in range(df.shape[0]):
            if df['country_code'][i] == 'DE':
                df['phone_number'][i] = f'+49 {df["phone_number"][i]}'
            elif df['country_code'][i] == 'GB':
                df['phone_number'][i] = f'+44 {df["phone_number"][i]}'
            elif df['country_code'][i] == 'US':
                df['phone_number'][i] = f'+1 {df["phone_number"][i]}'
            else:
                df.drop(index=[i], inplace=True)  # Drop rows with unsupported country codes
        df.dropna(inplace=True)  # Drop rows with missing values
        df.set_index('index', inplace=True)  # Set index column
        df.reset_index(drop=True, inplace=True)  # Reset index
        database_utils.DatabaseConnector().upload_to_db(df, 'dim_users')  # Upload cleaned user data to the database

    def clean_card_data(self):
        # Method to clean and upload card data from a PDF to the database
        df = data_extraction.DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        # Cleaning date and numeric columns
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', dayfirst=True, errors='coerce')
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df['card_number'] = df['card_number'].str.extract('(\d+)', expand=False)
        df.dropna(inplace=True)  # Drop rows with missing values
        database_utils.DatabaseConnector().upload_to_db(df, 'dim_card_details')  # Upload cleaned card data to the database

    def clean_store_data(self):
        # Method to clean and upload store data from API endpoints to the database
        df = data_extraction.DataExtractor().retrieve_stores_data(header, number_of_stores, retrieve_a_store)
        # Cleaning and standardizing store data
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', dayfirst=True, errors='coerce')
        for i in range(df.shape[0]):
            if df['country_code'][i] not in ['US', 'GB', 'DE']: df.drop(i, inplace=True)  # Drop rows with unsupported country codes
        df['continent'] = df['continent'].str.replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].str.replace('eeAmerica', 'America')
        df.reset_index(drop=True, inplace=True)  # Reset index
        database_utils.DatabaseConnector().upload_to_db(df, 'dim_store_details')  # Upload cleaned store data to the database 

    def convert_product_weights(self):
        # Method to convert and standardize product weights
        df = data_extraction.DataExtractor().extract_from_s3()
        df.set_index(df.columns[0], inplace=True)  # Set index column
        df = df[df['weight'].str.len() < 10]  # Filter out rows with long weight values
        # Cleaning and converting weight units
        df['weight'] = df['weight'].str.replace(r'\.$', '', regex=True)  # Removing trailing dots
        df['weight'] = df['weight'].str.replace('ml', 'g')  # Standardizing unit to grams before conversion to kg
        df['weight'][df['weight'].str.contains('oz', na=False)] = df['weight'].str.extractall(r'(\d+\.\d+|\d+)').astype(float, errors='raise').unstack().prod(axis=1) * 29.57  # Converting ounces to grams
        df['weight'][df['weight'].str.contains('x', na=False)] = df['weight'].str.extractall(r'(\d*\.?\d+)').astype(float, errors='raise').unstack().prod(axis=1)/1000  # Resolving multiplied values
        df['weight'] = df['weight'].apply(Quantity)/1000  # Standardizing weight units to kilograms
        return df

    def clean_products_data(self):
        # Method to clean and upload product data to the database
        df = self.convert_product_weights()  # Convert product weights
        df.index.name = 'index'  # Set index name
        df.dropna(inplace=True)  # Drop rows with missing values
        database_utils.DatabaseConnector().upload_to_db(df, 'dim_products')  # Upload cleaned product data to the database
    
    def clean_orders_data(self):
        # Method to clean and upload order data to the database
        df = data_extraction.DataExtractor().read_rds_table('orders_table')  # Reading order data from RDS database
        df.drop(['first_name', 'last_name', '1', 'level_0'], axis=1, inplace=True)  # Drop unnecessary columns
        df.set_index('index', inplace=True)  # Set index column
        database_utils.DatabaseConnector().upload_to_db(df, 'orders_table')  # Upload cleaned order data to the database

    def clean_date_details(self):
        # Method to clean and upload date details data to the database
        df = data_extraction.DataExtractor().retrieve_date_details()  # Reading date details from S3
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce').dt.time  # Convert timestamp to time format
        df.dropna(inplace=True)  # Drop rows with missing values
        database_utils.DatabaseConnector().upload_to_db(df, 'dim_date_times')  # Upload cleaned date details data to the database

processor = DataCleaning()  # Creating an instance of DataCleaning class

# uncomment method call as required:
# processor.clean_user_data()  # Clean and upload user data
# processor.clean_card_data()  # Clean and upload card data
# processor.clean_store_data()  # Clean and upload store data
# processor.clean_products_data()  # Clean and upload product data
# processor.clean_orders_data()  # Clean and upload order data
# processor.clean_date_details()  # Clean and upload date details
