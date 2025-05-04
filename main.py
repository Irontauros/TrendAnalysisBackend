import time
from dotenv import load_dotenv
import os
import mysql.connector
from stage1 import fetch_and_store_articles
from stage2 import fetch_and_update_data
from stage3 import run_prediction_pipeline

# Load environment variables from the .env file
load_dotenv()

# Get database configuration from environment variables
db_config = {
    'host': '34.175.68.152',
    'user': 'root',
    'password': 'Impala69!',
    'database': 'trend-analysis-db'
}


def main():

    # print("Starting the article processing pipeline...")

    # Stage 1: Fetch and store articles
    print("\n--- Stage 1: Fetching and storing articles ---")
    fetch_and_store_articles(db_config)

    # Stage 2: Enrich articles with AI tags and update totals
    print("\n--- Stage 2: Enriching articles and saving totals ---")
    #fetch_and_update_data(db_config)

    # Stage 3: Run prediction pipeline
    print("\n--- Stage 3: Running predictions for future data ---")
   #run_prediction_pipeline(db_config)

    print("\n✅ All stages complete!")

if __name__ == "__main__":
    main()
