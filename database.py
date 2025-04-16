from imports import *

# Charger les informations de connexion MySQL
path_to_mysql_creds = r"c:\Credentials\mysql_creds.json"
with open(path_to_mysql_creds, 'r') as file:
    content = json.load(file)
    mysql_user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]
    database = "oceanography_data_analysis"
    silver_table = "silver table"

metadata = MetaData()

table_dim_station = "dim_station"
table_dim_time = "dim_time"
table_facts_meteo = "facts_meteo"
table_facts_ocean = "facts_ocean"
# Connexion à MySQL avec SQLModel
engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{password}@{host}:{port}/{database}", isolation_level='AUTOCOMMIT')
Session = sessionmaker(bind=engine)
session = Session()

table_dim_station = Table(table_dim_station, metadata, autoload_with=engine)
table_dim_time = Table(table_dim_time, metadata, autoload_with=engine)
table_facts_meteo = Table(table_facts_meteo, metadata, autoload_with=engine)
table_facts_ocean = Table(table_facts_ocean, metadata, autoload_with=engine)
table_silver = Table(silver_table, metadata, autoload_with=engine)

# fonction pour chercher une colonne définie dans une table
def find_column(table, column_name):
    if column_name in table.c:
        return table.c.get(column_name, None)
    return

dim_time_datetime = table_dim_time.c.Datetime


