from astrapy import DataAPIClient
from dotenv import load_dotenv
import os

load_dotenv()

ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")
KEYSPACE_NAME = "streaming"

def connected_cassandra():
    client = DataAPIClient(ASTRA_DB_TOKEN)
    db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT, keyspace=KEYSPACE_NAME)
    collections = db.list_collection_names()
    print(f"Connected to Astra DB: {collections}")
    return db
