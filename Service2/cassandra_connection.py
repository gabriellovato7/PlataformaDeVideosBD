from astrapy import DataAPIClient
from dotenv import load_dotenv
import os

load_dotenv()

ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")

client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)
collections = db.list_collection_names()
print(f"Connected to Astra DB: {collections}")
