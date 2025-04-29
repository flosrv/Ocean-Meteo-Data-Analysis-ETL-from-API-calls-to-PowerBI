from sqlmodel import SQLModel, Field, create_engine, Session
from typing import Optional
from datetime import datetime
import json

# ==============================
# Charger les informations de connexion
# ==============================
creds_path: str = r"c:\Credentials\mysql_creds.json"
with open(creds_path, 'r') as file:
    content = json.load(file)
    mysql_user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]

# ==============================
# Nom de la base de données
# ==============================
db_DW = 'oceanography_data_analysis'

# ==============================
# Création de l'engine pour se connecter à la base de données existante
# ==============================
engine_DW = create_engine(f"mysql://{mysql_user}:{password}@{host}/{db_DW}", echo=True)

# ==============================
# Modèles de données pour SQLModel
# ==============================

# Modèle pour DimStation
class DimStation(SQLModel, table=True):
    __tablename__ = 'dim_station'
    
    index: int = Field(primary_key=True)
    station_id: Optional[str] = Field(default=None, alias="Station ID")
    station_zone: Optional[str] = Field(default=None, alias="Station Zone")
    lat: Optional[str] = Field(default=None, alias="Lat")
    lon: Optional[str] = Field(default=None, alias="Lon")

    class Config:
        orm_mode = True  # Permet d'utiliser avec Pydantic

# Modèle pour DimTime
class DimTime(SQLModel, table=True):
    __tablename__ = 'dim_time'
    
    index: int = Field(primary_key=True)
    datetime_value: Optional[datetime] = Field(default=None, alias="Datetime")  # Renommé en datetime_value
    year: Optional[str] = Field(default=None, alias="Year")
    month: Optional[str] = Field(default=None, alias="Month")
    day_of_week: Optional[str] = Field(default=None, alias="DayOfWeek")
    day: Optional[str] = Field(default=None, alias="Day")
    hour: Optional[str] = Field(default=None, alias="Hour")
    day_period: Optional[str] = Field(default=None, alias="DayPeriod")

    class Config:
        orm_mode = True

# Modèle pour FactsMeteo
class FactsMeteo(SQLModel, table=True):
    __tablename__ = 'facts_meteo'
    
    unique_id: Optional[str] = Field(default=None, alias="Unique ID")
    temperature_c: Optional[float] = Field(default=None, alias="T°(C°)")
    humidity: Optional[float] = Field(default=None, alias="Relative Humidity (%)")
    dew_point: Optional[float] = Field(default=None, alias="Dew Point (°C)")
    precipitations: Optional[float] = Field(default=None, alias="Precipitations (mm)")
    pressure: Optional[float] = Field(default=None, alias="Sea Level Pressure (hPa)")
    low_clouds: Optional[float] = Field(default=None, alias="Low Clouds (%)")
    middle_clouds: Optional[float] = Field(default=None, alias="Middle Clouds (%)")
    high_clouds: Optional[float] = Field(default=None, alias="High Clouds (%)")
    cloud_cover: Optional[float] = Field(default=None, alias="Cloud Cover (%)")
    visibility: Optional[float] = Field(default=None, alias="Visibility (km)")
    wind_speed: Optional[float] = Field(default=None, alias="Wind Speed (10m)")
    wind_dir: Optional[float] = Field(default=None, alias="Wind Direction (°)")
    wind_gusts: Optional[float] = Field(default=None, alias="Wind Gusts (km/h)")
    baro_elevation: Optional[str] = Field(default=None, alias="Barometer Elevation (m)")
    air_temp_height: Optional[str] = Field(default=None, alias="Air T° Height (m)")
    station_id: Optional[str] = Field(default=None, alias="Station ID")
    datetime_value: Optional[datetime] = Field(default=None, alias="Datetime")  # Renommé ici aussi

    class Config:
        orm_mode = True

# Modèle pour FactsOcean
class FactsOcean(SQLModel, table=True):
    __tablename__ = 'facts_ocean'
    
    unique_id: Optional[str] = Field(default=None, alias="Unique ID")
    wave_height: Optional[float] = Field(default=None, alias="Wave Height (m)")
    wave_period: Optional[float] = Field(default=None, alias="Average Wave Period (s)")
    wave_direction: Optional[float] = Field(default=None, alias="Dominant Wave Direction (°)")
    water_temp: Optional[float] = Field(default=None, alias="Water T° (°C)")
    water_depth: Optional[float] = Field(default=None, alias="Water Depth (m)")
    sea_temp_depth: Optional[str] = Field(default=None, alias="Sea Temperature Depth (m)")
    station_id: Optional[str] = Field(default=None, alias="Station ID")
    datetime_value: Optional[datetime] = Field(default=None, alias="Datetime")  # Renommé ici aussi

    class Config:
        orm_mode = True
