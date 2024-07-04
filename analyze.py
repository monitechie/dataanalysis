from pymongo import MongoClient
import pandas as pd

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['covid19']
collection = db['data']


def process_data():
    # Fetch data from MongoDB
    data = list(collection.find())
    df = pd.DataFrame(data)

    # Perform analysis (e.g., calculate total cases, deaths, and recoveries)
    summary = df[['country', 'cases', 'deaths', 'recovered']]

    # Save the processed data to a CSV file for Power BI
    summary.to_csv('covid19_summary.csv', index=False)
    print("Data processed and saved successfully!")


if __name__ == "__main__":
    process_data()
