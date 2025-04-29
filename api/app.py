from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routers import marine, meteo
from database import engine_staging, cleaned_marine_data, cleaned_meteo_data
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect

# =============================
# LIFESPAN CONTEXT
# =============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialise les tables et la connexion à la base de données pendant le cycle de vie de l'application.
    """
    # Création de la session pour interagir avec la DB (Database Session)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_staging)
    app.state.db = SessionLocal()

    # Vérification de la disponibilité des tables
    inspector = inspect(engine_staging)
    tables = inspector.get_table_names()

    # Mise à disposition des tables pour l'application
    app.state.tables = {
        'cleaned_marine_data': cleaned_marine_data,
        'cleaned_meteo_data': cleaned_meteo_data,
    }
    app.state.tables_available = {table: table in tables for table in app.state.tables}

    # Point de reprise de l'application
    yield

    # Fermeture de la session à la fin
    app.state.db.close()
    app.state.tables = None
    app.state.tables_available = None


# =============================
# APP FASTAPI
# =============================
app = FastAPI(lifespan=lifespan)

# =============================
# ROUTERS
# =============================
app.include_router(marine.router, prefix="/marine", tags=["Marine Data"])
app.include_router(meteo.router, prefix="/meteo", tags=["Meteo Data"])

# =============================
# ROUTES DE TEST
# =============================
@app.get("/startup")
async def startup():
    """
    Vérifie que l'application a bien démarré et que les tables sont disponibles.
    """
    return {
        "message": "API démarrée avec succès",
        "tables": list(app.state.tables.keys()),
        "tables_available": app.state.tables_available
    }

@app.get("/tables")
async def get_tables():
    """
    Renvoie la liste des tables disponibles dans l'application.
    """
    return {
        "tables": list(app.state.tables.keys()),
        "tables_available": app.state.tables_available
    }
