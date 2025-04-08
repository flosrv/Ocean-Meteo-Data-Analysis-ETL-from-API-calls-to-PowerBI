from imports import *
from functions import *

with open("buoy_near_SM.json", "r") as f:
    buoy_near_SM = json.load(f)


path_postgresql_creds = r"C:\Users\f.gionnane\Documents\Data Engineering\Credentials\postgresql_creds.json"
with open(path_postgresql_creds, 'r') as file:
    content = json.load(file)
    user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

# Créer l'engine PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
conn = engine.connect()

db = buoy_near_SM["db"]
schema = buoy_near_SM["schema"] 

bronze_marine_data_table_name = buoy_near_SM["bronze_marine"] 
bronze_meteo_data_table_name = buoy_near_SM["bronze_meteo"] 

def fetch_table_data(conn, schema, table_name):
    """
    Récupère toutes les données d'une table PostgreSQL et les charge dans un DataFrame Pandas.

    :param conn: Connexion SQLAlchemy active à la base de données PostgreSQL.
    :param schema: Nom du schéma contenant la table.
    :param table_name: Nom de la table à récupérer.
    :return: DataFrame contenant les données de la table.
    """
    query = text(f'SELECT * FROM "{schema}"."{table_name}"')
    df = pd.read_sql(query, conn)
    return df

try:
    df_marine_to_clean = fetch_table_data(conn=conn, schema=schema, table_name= bronze_marine_data_table_name)
    df_meteo_to_clean = fetch_table_data(conn=conn, schema=schema, table_name= bronze_meteo_data_table_name)
except Exception as e:
    print(e)






















