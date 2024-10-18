import os
import requests
import pandas as pd
from sqlalchemy import create_engine, NVARCHAR, INTEGER, BOOLEAN, FLOAT
from dotenv import load_dotenv

# Load sensitive info from .env file
load_dotenv()

# API credentials
API_KEY = os.getenv('RAPIDAPI_KEY')
API_HOST = os.getenv('RAPIDAPI_HOST')

# Database credentials
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')

# Base URL for the API
API_URL = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"

# Headers for the API request
HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

# Function to fetch data from the API


def fetch_data(page, category_id="16333372011", country="US", sort_by="RELEVANCE", condition="ALL", prime="false", discounts="NONE"):
    """Fetch data from the API for a given page."""
    querystring = {
        "category_id": category_id,
        "page": page,
        "country": country,
        "sort_by": sort_by,
        "product_condition": condition,
        "is_prime": prime,
        "deals_and_discounts": discounts
    }

    try:
        response = requests.get(API_URL, headers=HEADERS, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# Function to process the fetched data


def process_data(data):
    """Process raw data into a Pandas DataFrame."""
    if 'data' in data and 'products' in data['data']:
        return pd.DataFrame(data['data']['products'])
    return pd.DataFrame()

# Function to clean the data


def clean_data(df):
    """Clean and transform the fetched data."""
    df = df.drop(columns=['currency', 'product_url', 'product_photo',
                 'delivery', 'unit_price', 'unit_count', 'coupon_text'])

    # Removing dollar signs and converting to numeric
    for column in ['product_price', 'product_original_price', 'product_minimum_offer_price']:
        df[column] = pd.to_numeric(
            df[column].str.replace('$', ''), errors='coerce')

    return df

# Function to store data in the database


def store_data(df, table_name='product_by_category'):
    """Store cleaned data into the SQL database."""
    engine = create_engine(
        f'mssql+pyodbc://{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server')

    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype={
        'asin': NVARCHAR(50),
        'product_title': NVARCHAR(),
        'product_price': FLOAT(),
        'product_original_price': FLOAT(),
        'product_star_rating': FLOAT(),
        'product_num_ratings': INTEGER(),
        'product_num_offers': INTEGER(),
        'product_minimum_offer_price': FLOAT(),
        'is_best_seller': BOOLEAN(),
        'is_amazon_choice': BOOLEAN(),
        'is_prime': BOOLEAN(),
        'climate_pledge_friendly': BOOLEAN(),
        'sales_volume': NVARCHAR(),
        'has_variations': BOOLEAN(),
        'product_availability': NVARCHAR()
    })

# Main function to fetch, clean, and store the data


def main():
    products_data = []

    for i in range(1, 100):  # Loop through the first 100 pages
        data = fetch_data(i)
        if data:
            df_page = process_data(data)
            if not df_page.empty:
                products_data.append(df_page)

    # Combine all pages into a single DataFrame
    if products_data:
        df_all = pd.concat(products_data, ignore_index=True)
        cleaned_df = clean_data(df_all)

        # Store cleaned data to the database
        store_data(cleaned_df)

        # Save cleaned data to CSV
        cleaned_df.to_csv('final_data_to_db.csv', index=False)
    else:
        print("No data to process.")


if __name__ == "__main__":
    main()
