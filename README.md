# Multinational Rretail Data Centralisation

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Description
This project focuses on extracting, cleaning, and storing data from various sources using Python.
  It includes classes and methods to connect to databases, extract data from AWS S3 buckets,
  APIs, and PDFs, clean the data, and upload it to a PostgreSQL database.

The aim of the project is to demonstrate proficiency in data engineering tasks, including data extraction,
  transformation, and loading (ETL), as well as working with different data formats and sources.

Throughout the project, you will learn how to:
- Connect to databases and extract data using SQLAlchemy
- Extract data from AWS S3 buckets using Boto3
- Extract data from APIs using requests library
- Extract data from PDFs using tabula-py
- Clean and transform data using pandas
- Upload cleaned data to a PostgreSQL database

## Installation
To run the project, follow these steps:

1. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/HBakkich/multinational-retail-data-centralisation892.git
   ```
2. Navigate to the project directory:
   ```bash
   cd multinational-retail-data-centralisation892
   ```
3. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```
4. Ensure you have AWS CLI installed and configured with appropriate credentials for accessing S3 buckets.

Note: A yaml credentials file has been added to gitignore which contains the necessary creds for creating required connections.

## Usage
The project consists of several Python scripts and classes. Here's how to use them:

1. **Data Extraction:**
   - Use `data_extraction.py` to extract data from various sources such as databases, S3 buckets, APIs, and PDFs.
   
2. **Data Cleaning:**
   - Use `data_cleaning.py` to clean the extracted data, including handling missing values, data formatting, and standardization.
   
3. **Database Interaction:**
   - Use `database_utils.py` to connect to a PostgreSQL database and upload cleaned data.

4. **Tasks Execution:**
   - Execute the methods defined in the classes to perform specific tasks such as cleaning user data, card data, store data, product data, orders data, and date events data.

## File Structure
The project directory structure is organized as follows:
```
project_root/
│
├── data_extraction.py
├── data_cleaning.py
├── database_utils.py
├── README.md
├── requirements.txt
│
└── .gitignore
```

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
```
