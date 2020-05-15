import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

def get_posgres_connection():
    db_name = os.getenv("PSQL_DB_NAME")
    db_user = os.getenv("PSQL_DB_USER")
    db_password = os.getenv("PSQL_DB_PASSWORD")
    db_host = os.getenv("PSQL_DB_HOST")
    sql_engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')
    return sql_engine