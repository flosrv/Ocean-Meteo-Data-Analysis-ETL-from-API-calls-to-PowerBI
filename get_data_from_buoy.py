from functions import *
from imports import *

path_postgresql_creds = r"C:\Users\f.gionnane\Documents\Data Engineering\Credentials\postgresql_creds.json"
with open(path_postgresql_creds, 'r') as file:
    content = json.load(file)
    user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

db = "MyProjects"
schema = "End_To_End_Oceanography_ML"

# Créer l'engine PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
conn = engine.connect()


lat_SM = "11.24N"
lon_SM = "74.21W"

nearest = api.nearest_station(lat_SM,lon_SM)

buoy_near_SM = api.station(station_id=nearest, as_df=False)

Buoy_Location = buoy_near_SM['Location']
lat_buoy = float(Buoy_Location.split(' ')[0])  # Convertir en float
lon_buoy = float(Buoy_Location.split(' ')[2])  # Convertir en float

lat_buoy = round(lat_buoy, 2)  # Arrondir à 2 décimales
lon_buoy = round(lon_buoy, 2)


# Extraction du nom de la station et de la zone
station_name = buoy_near_SM['Name'].split('-')[0].strip()  # Utilisez buoy_near_SM['Name']
station_zone = buoy_near_SM['Name'].split('-')[1].strip()

df_marine = NDBC.realtime_observations(nearest)

bronze_marine_table_name = f"Bronze_marine_data_{station_name.replace(' ', '_')}_{station_zone.replace(' ', '_')}_{lat_buoy}_{lon_buoy}"
bronze_marine_table_name = bronze_marine_table_name.replace('.', '-')

load_data_in_table(db=db, schema=schema, table_name=bronze_marine_table_name, df=df_marine,conn=conn,key_column='time')

coordinates = [lat_buoy, lon_buoy]
df_meteo = meteo_api_request(coordinates=coordinates)

# Création du nom de la table
bronze_meteo_data_table_name = f"bronze_meteo_data_{station_name.replace(' ', '_')}_{station_zone.replace(' ', '_')}_{lat_buoy}_{lon_buoy}"
bronze_meteo_data_table_name = bronze_meteo_data_table_name.replace('.', '-')
load_data_in_table(db=db, schema=schema, table_name=bronze_meteo_data_table_name, df=df_marine,conn=conn,key_column='time')












lat_SM = "11.24N"
lon_SM = "74.21W"

nearest = api.nearest_station(lat_SM,lon_SM)

buoy_near = api.station(station_id=nearest, as_df=False)

Buoy_Location = buoy_near['Location']
lat_buoy = float(Buoy_Location.split(' ')[0])  # Convertir en float
lon_buoy = float(Buoy_Location.split(' ')[2])  # Convertir en float

lat_buoy = round(lat_buoy, 2)  # Arrondir à 2 décimales
lon_buoy = round(lon_buoy, 2)

# Extraction du nom de la station et de la zone
station_name = buoy_near['Name'].split('-')[0].strip()
station_zone = buoy_near['Name'].split('-')[1].strip()

# Création du nom de la table
table_name = f"{station_name.replace(' ', '_')}_{station_zone.replace(' ', '_')}_{lat_buoy}_{lon_buoy}"

# Affichage des résultats
print(f'{lat_buoy}, {lon_buoy}, {station_name}, \ntable name : {table_name}')

lat_buoy = round(lat_buoy,2)
lon_buoy = round(lon_buoy, 2)

df_marine = NDBC.realtime_observations(nearest)

df_resampled = process_and_resample(df_marine, 'time')

df_marine = handle_null_values(df_marine)

coordinates = [lat_buoy, lon_buoy]
df_meteo = meteo_api_request(coordinates=coordinates)

df_meteo =process_and_resample(df_meteo, 'date')

df_meteo.loc[:, df_meteo.select_dtypes(include=['float32', 'float64']).columns] = df_meteo.select_dtypes(include=['float32', 'float64']).applymap(lambda x: round(x, 2))

df_meteo = handle_null_values(df_meteo)
df_marine = handle_null_values(df_marine)

df_meteo = drop_columns_if_exist(df_meteo,['rain', 'showers','soil_moisture_0_to_1cm', 'cloud_cover', 'soil_temperature_0cm',	'soil_moisture_0_to_1cm', 'is_day'])

df_meteo.rename(columns={'temperature_2m': 'T°(C°)', 
                         'relative_humidity_2m': 'Relative Humidity (%)',
                         'dew_point_2m': 'Dew Point (°C)', 
                         'precipitation': 'Precipitation (mm)', 
                         'pressure_msl':' Sea Level Pressure (hPa)', 
                         'cloud_cover_low':'Low Clouds (%)',
                         'cloud_cover_mid' : 'Middle Clouds (%)',	
                         'cloud_cover_high' : 'High Clouds (%)', 
                         'visibility' : ' Visibility (%)', 
                         'wind_speed_10m' : 'Wind Speed (km/h)'}, 
                         inplace=True)
df_marine.rename(columns={
    'wind_direction': 'Wind Direction (°)',
    'wind_speed': 'Wind Speed (km/h)',
    'wind_gust': 'Wind Gusts (km/h)',
    'wave_height': 'Wave Height (m)',
    'average_wave_period': 'Average Wave Period (s)',
    'dominant_wave_direction': 'Dominant Wave Direction (°)',
    'pressure': 'Pressure (hPA)',
    'air_temperature': 'Air T°',
    'water_temperature': 'Water T°'}, 
    inplace=True)

df_merged = pd.merge(df_marine, df_meteo, on = 'Datetime', how='inner')

df_merged = add_daytime_and_month_column(df_merged,'Datetime')

df_merged['Wind Speed (km/h)'] = (df_merged['Wind Speed (km/h)_x']+ df_merged['Wind Speed (km/h)_y'])/2
df_merged = drop_columns_if_exist(df_merged, ['Wind Speed (km/h)_x', 'Wind Speed (km/h)_y', 'Wind Gusts (km/h)'])

from sqlalchemy import create_engine, text
import json

# Charger les informations de connexion PostgreSQL
path_postgresql_creds = r"C:\Users\f.gionnane\Documents\Data Engineering\Credentials\postgresql_creds.json"
with open(path_postgresql_creds, 'r') as file:
    content = json.load(file)
    user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

db = "MyProjects"
schema = "End_To_End_Oceanography_ML"

# Créer l'engine PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
conn = engine.connect()

# Connexion à la base de données
load_data_in_table(conn, db, schema, table_name, df = df_merged, key_column='Datetime')