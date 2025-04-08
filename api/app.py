import json
from fastapi import FastAPI
from sqlalchemy import create_engine, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from api.main_db import engine, metadata_silver
from contextlib import asynccontextmanager

# Définir la base pour déclarer les modèles (tables)
Base = declarative_base()

# Créer une application FastAPI
app = FastAPI()

# Variable pour stocker les tables du schéma "Silver"
tables_silver = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connexion à la base de données
    metadata_silver.reflect(bind=engine)
    # Charger les tables du schéma "Silver" dans un dictionnaire
    app.state.tables = {table.name: table for table in metadata_silver.sorted_tables}
    yield
    # Fermeture de la connexion à la base de données
    app.state.tables = None

# Création de l'application FastAPI avec le lifespan
app = FastAPI(lifespan=lifespan)

@app.on_event("startup")
async def startup():
    # Cette fonction sera exécutée lors du démarrage de l'API
    # Vous pouvez accéder à app.state.tables après le démarrage
    print("Tables chargées :", app.state.tables)

# Exemple d'endpoint pour tester l'accès aux tables
@app.get("/tables")
async def get_tables():
    # Retourner les tables du schéma "Silver"
    return {"tables": list(app.state.tables.keys())}
