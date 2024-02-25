import pandas as pd
import database_utils
import tabula
import requests
import json
import boto3

class DataExtractor:
    def __init__(self) -> None:
        pass  # No initialization logic required
    def read_rds_table(self, table_name):
        # Method to read data from a specified table in the RDS database
        db_connector =  database_utils.DatabaseConnector()  # Creating an instance of DatabaseConnector
        return pd.read_sql_table(table_name, db_connector.init_db_engine())  # Reading table data into a DataFrame
    
    def retrieve_pdf_data(self, link):
        # Method to retrieve data from a PDF file given its link
        return pd.DataFrame(tabula.read_pdf(link, pages='all', multiple_tables=False)[0])  # Extracting data from PDF into a DataFrame
    
    def list_number_of_stores(self, header_dict, number_of_stores_endpoint):
        # Method to get the number of stores from an API endpoint
        response = requests.get(number_of_stores_endpoint, headers=header_dict)  # Making GET request to endpoint
        return json.loads(response.text)['number_stores']  # Parsing JSON response to get the number of stores

    def retrieve_stores_data(self, header_dict, number_of_stores_endpoint, retrieve_a_store_endpoint):
        # Method to retrieve data for all stores from API endpoints
        lst = []
        num_stores = self.list_number_of_stores(header_dict, number_of_stores_endpoint)  # Getting number of stores
        for i in range(num_stores):
            # Iterating over all store endpoints to retrieve data
            response = requests.get(retrieve_a_store_endpoint+str(i), headers=header_dict)  # Making GET request
            lst.append(json.loads(response.text))  # Appending JSON response to list
        df = pd.DataFrame(lst)  # Creating DataFrame from list of store data
        df.set_index('index', inplace=True)  # Setting 'index' column as index of DataFrame
        return df
    def extract_from_s3(self):
        # Method to extract data from an S3 bucket
        AWS_S3_BUCKET = 'data-handling-public'  # S3 bucket name
        s3 = boto3.client('s3')  # Creating an S3 client
        obj = s3.get_object(Bucket=AWS_S3_BUCKET, Key='products.csv')  # Getting object from S3
        df = pd.read_csv(obj['Body'])  # Reading CSV file from S3 into a DataFrame
        return df
    def retrieve_date_details(self):
        # Method to retrieve date details from a JSON file hosted on S3
        return pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
