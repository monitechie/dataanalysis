import requests
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import time

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['covid19']
collection = db['data']

url = 'https://disease.sh/v3/covid-19/countries'


def fetch_data(url, retries=3, delay=5):
    for i in range(retries):
        try:
            response = requests.get(url)
            # Raise an HTTPError if the HTTP request returned an unsuccessful status code
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Attempt {i + 1} failed: {e}")
            time.sleep(delay)
    raise ConnectionError(f"Failed to fetch data after {retries} retries.")


try:
    data = fetch_data(url)

    # Process and store data
    for country_data in data:
        # Using current datetime since API does not provide a timestamp
        country_data['Date'] = datetime.now()
        collection.update_one({'country': country_data['country']}, {
                              '$set': country_data}, upsert=True)

    print("Data updated successfully!")
except ConnectionError as e:
    print(f"Failed to fetch data: {e}")
