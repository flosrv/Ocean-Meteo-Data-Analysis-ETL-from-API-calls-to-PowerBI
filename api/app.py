from fastapi import FastAPI
from imports import *
from database import *
from functions import *
from api.routes import router  # Assurez-vous que le routeur est correctement importé
from api.sql_models import DimStation, DimTime, FactsMeteo, FactsOcean  # Importer les modèles SQLModel

# Créer une application FastAPI
app = FastAPI()

# Variable pour stocker les tables du schéma
tables = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Contexte de vie pour l'application FastAPI.
    Cette fonction est utilisée pour la gestion de la connexion à la base de données.
    """
    # Connexion à la base de données
    metadata.reflect(bind=engine_DW)

    # Charger les tables associées aux modèles SQLModel (par exemple, DimStation, DimTime, etc.)
    app.state.tables = {
        'dim_station': DimStation,
        'dim_time': DimTime,
        'facts_meteo': FactsMeteo,
        'facts_ocean': FactsOcean,
    }
    
    # Laisser l'application tourner pendant le cycle de vie
    yield
    
    # Fermeture de la connexion à la base de données après que l'application a cessé de fonctionner
    app.state.tables = None

# Création de l'application FastAPI avec le lifespan
app = FastAPI(lifespan=lifespan)

# Inclusion des routes dans l'application
app.include_router(router)

@app.get("/startup")
async def startup():
    """
    Cette fonction permet de tester l'accès aux tables et vérifie que l'application a bien chargé les tables au démarrage.
    """
    return {"message": "API démarrée avec succès", "tables": list(app.state.tables.keys())}

# Exemple d'endpoint pour tester l'accès aux tables
@app.get("/tables")
async def get_tables():
    """
    Cette route renvoie la liste des noms des tables associées aux modèles SQLModel.
    Elle permet de vérifier que l'application a bien chargé les tables.
    """
    return {"tables": list(app.state.tables.keys())}
