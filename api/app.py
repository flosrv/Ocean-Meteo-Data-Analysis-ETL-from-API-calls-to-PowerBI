from imports import *

# Créer une application FastAPI
app = FastAPI()

# Variable pour stocker les tables du schéma "Silver"
tables_silver = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connexion à la base de données
    metadata.reflect(bind=engine)
    # Charger les tables du schéma "Silver" dans un dictionnaire
    app.state.tables = {table.name: table for table in metadata.sorted_tables}
    yield
    # Fermeture de la connexion à la base de données
    app.state.tables = None

# Création de l'application FastAPI avec le lifespan
app = FastAPI(lifespan=lifespan)

app.include_router(router)

@app.get("startup")
async def startup():
    # Cette fonction sera exécutée lors du démarrage de l'API
    # Vous pouvez accéder à app.state.tables après le démarrage
    print("Tables chargées :", app.state.tables)

# Exemple d'endpoint pour tester l'accès aux tables
@app.get("/tables")
async def get_tables():
    # Retourner les tables du schéma "Silver"
    return {"tables": list(app.state.tables.keys())}
