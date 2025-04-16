from enum import Enum
from typing import List, Optional


# Dropdown using Enum for numeric columns
class NumericColumns(str, Enum):
    wind_speed = "Wind Speed (10m)"
    wind_direction = "Wind Direction (°)"
    wind_gusts = "Wind Gusts (km/h)"
    wave_height = "Wave Height (m)"
    avg_wave_period = "Average Wave Period (s)"
    dominant_wave_dir = "Dominant Wave Direction (°)"
    water_temp = "Water T° (°C)"
    water_depth = "Water Depth (m)"
    air_temp = "T°(C°)"
    dew_point = "Dew Point (°C)"
    relative_humidity = "Relative Humidity (%)"
    precipitations = "Precipitations (mm)"
    cloud_cover = "Cloud Cover (%)"
    low_clouds = "Low Clouds (%)"
    middle_clouds = "Middle Clouds (%)"
    high_clouds = "High Clouds (%)"
    surface_pressure = "Sea Level Pressure (hPa)"
    visibility = "Visibility (km)"
     
class CatColumns(str, Enum):
    lat = "Lat"
    lon = "Lon"
    station_id = "Station ID"
    station_zone = "Station Zone"
    sea_temperature_depth = "Sea Temperature Depth (m)"
    air_temp_height = "Air T° Height (m)"
    barometer_elevation = "Barometer Elevation (m)"
    year = "Year"
    month = "Month"
    day_of_week = "DayOfWeek"
    day_period = "DayPeriod"
    