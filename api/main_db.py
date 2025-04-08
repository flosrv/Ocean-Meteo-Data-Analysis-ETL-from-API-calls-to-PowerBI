from fastapi import FastAPI
from sqlalchemy import create_engine, MetaData
import json
# Charger les informations de connexion
path_postgresql_creds = r"C:\Credentials\postgresql_creds.json"

with open(path_postgresql_creds, 'r') as file:
    content = json.load(file)
    user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

db = "Oceanography_ML_Project"
schema_silver = "Silver"

# Créer l'engine PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
metadata_silver = MetaData(schema=schema_silver)

