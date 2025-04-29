from imports import *

# ==============================
# Connexion
# ==============================
creds_path: str = r"c:\Credentials\mysql_creds.json"
with open(creds_path, 'r') as file:
    content = json.load(file)
    mysql_user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

db_staging = 'db_staging'
db_DW = 'oceanography_data_analysis'

table_staging_marine_name = "cleaned_marine_data"
table_staging_meteo_name = "cleaned_meteo_data"

table_facts_marine_name = "facts_ocean"
table_facts_meteo_name = "facts_meteo"

table_dim_station_name = "dim_station"
table_dim_time_name = "dim_time"

# Test de la connexion pour le data warehouse
try:
    engine_DW = create_engine(f"mysql://{mysql_user}:{password}@{host}/{db_DW}")
    with engine_DW.connect() as connection:
        print("Connexion réussie à la base de données DW!")
except Exception as e:
    print(f"Erreur de connexion à la base de données DW: {e}")

# Test de la connexion pour la staging DB
try:
    engine_staging = create_engine(f"mysql://{mysql_user}:{password}@{host}/{db_staging}")
    with engine_staging.connect() as connection:
        print("Connexion réussie à la base de données Staging!")
except Exception as e:
    print(f"Erreur de connexion à la base de données Staging: {e}")


SessionLocalStaging = sessionmaker(bind=engine_staging)

def get_db_staging():
    db = SessionLocalStaging()
    try:
        yield db
    finally:
        db.close()

# Définition des tables
metadata = MetaData()

# ==============================
# Table: cleaned_marine_data
# ==============================
cleaned_marine_data = Table(
    'cleaned_marine_data', metadata,
    Column('Datetime', DateTime, nullable=True, primary_key=True),
    Column('Lat', String(255), nullable=True),
    Column('Lon', String(255), nullable=True),
    Column('Wave Height (m)', Float, nullable=True),
    Column('Average Wave Period (s)', Float, nullable=True),
    Column('Dominant Wave Direction (°)', Float, nullable=True),
    Column('Water T° (°C)', Float, nullable=True),
    Column('Water Depth (m)', Float, nullable=True),
    Column('Station ID', String(255), nullable=True),
    Column('Station Zone', String(255), nullable=True),
    Column('Sea Temperature Depth (m)', String(255), nullable=True),
    Column('Barometer Elevation (m)', String(255), nullable=True),
    Column('Sea Level Pressure (hPa)', Float, nullable=True),
    Column('Year', String(255), nullable=True),
    Column('Month', String(255), nullable=True),
    Column('Day', String(255), nullable=True),
    Column('Hour', String(255), nullable=True),
    Column('DayOfWeek', String(255), nullable=True),
    Column('DayPeriod', String(255), nullable=True)
)


# ==============================
# Table: cleaned_meteo_data
# ==============================
cleaned_meteo_data = Table(
    'cleaned_meteo_data', metadata,
    Column('Datetime', DateTime, nullable=True, primary_key=True),
    Column('Lat', String(255), nullable=True),
    Column('Lon', String(255), nullable=True),
    Column('Wind Direction (°)', Float, nullable=True),
    Column('Wind Gusts (km/h)', Float, nullable=True),
    Column('Station ID', String(255), nullable=True),
    Column('Station Zone', String(255), nullable=True),
    Column('Sea Temperature Depth (m)', String(255), nullable=True),
    Column('Barometer Elevation (m)', String(255), nullable=True),
    Column('Air T° Height (m)', String(255), nullable=True),
    Column('T°(C°)', Float, nullable=True),
    Column('Relative Humidity (%)', Float, nullable=True),
    Column('Dew Point (°C)', Float, nullable=True),
    Column('Precipitations (mm)', Float, nullable=True),
    Column('Cloud Cover (%)', Float, nullable=True),
    Column('Low Clouds (%)', Float, nullable=True),
    Column('Middle Clouds (%)', Float, nullable=True),
    Column('High Clouds (%)', Float, nullable=True),
    Column('Visibility (km)', Float, nullable=True),
    Column('Wind Speed (10m)', Float, nullable=True),
    Column('Year', String(255), nullable=True),
    Column('Month', String(255), nullable=True),
    Column('Day', String(255), nullable=True),
    Column('Hour', String(255), nullable=True),
    Column('DayOfWeek', String(255), nullable=True),
    Column('DayPeriod', String(255), nullable=True)
)
