import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Database connection function."""
    return pymysql.connect(
        host=os.getenv("db_host", "localhost"),
        port=int(os.getenv("db_port", 3306)),
        user=os.getenv("db_user"),
        password=os.getenv("db_password"),
        database=os.getenv("db_database"), 
        cursorclass=pymysql.cursors.DictCursor
    ) 