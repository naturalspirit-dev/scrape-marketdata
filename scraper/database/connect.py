from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from dotenv import load_dotenv
from pathlib import Path
basepath = Path()
basedir = str(basepath.cwd())
envars = basepath.cwd() / '.env'
load_dotenv(envars)

def getDatabase():
    CONNECTION_STRING = f"mongodb+srv://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@cluster0.hcvfmxl.mongodb.net/test?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)
    try:
        client.admin.command('ping')
    except ConnectionFailure:
        print("Server not available")
    return client
    
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
    dbname = getDatabase()