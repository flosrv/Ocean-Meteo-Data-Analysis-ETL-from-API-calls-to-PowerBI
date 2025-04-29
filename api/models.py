from enum import Enum

# ==============================
# Enums pour cleaned_marine_data
# ==============================


# Champs numériques (float)
class MarineNumericFields(Enum):
    wave_height = "Wave Height (m)"
    wave_period = "Average Wave Period (s)"
    wave_direction = "Dominant Wave Direction (°)"
    water_temp = "Water T° (°C)"
    water_depth = "Water Depth (m)"
    sea_level_pressure = "Sea Level Pressure (hPa)"

# Champs non numériques (string, datetime, etc.)
class MarineNonNumericFields(Enum):
    datetime = "Datetime"
    lat = "Lat"
    lon = "Lon"
    station_id = "Station ID"
    station_zone = "Station Zone"
    sea_temp_depth = "Sea Temperature Depth (m)"
    baro_elevation = "Barometer Elevation (m)"
    year = "Year"
    month = "Month"
    day = "Day"
    hour = "Hour"
    day_of_week = "DayOfWeek"
    day_period = "DayPeriod"


# ==============================
# Enums pour cleaned_meteo_data
# ==============================


# Champs numériques (float)
class MeteoNumericFields(Enum):
    wind_direction = "Wind Direction (°)"
    wind_gusts = "Wind Gusts (km/h)"
    temperature_c = "T°(C°)"
    relative_humidity = "Relative Humidity (%)"
    dew_point = "Dew Point (°C)"
    precipitations = "Precipitations (mm)"
    cloud_cover = "Cloud Cover (%)"
    low_clouds = "Low Clouds (%)"
    middle_clouds = "Middle Clouds (%)"
    high_clouds = "High Clouds (%)"
    visibility = "Visibility (km)"
    wind_speed = "Wind Speed (10m)"

# Champs non numériques (string, datetime, etc.)
class MeteoNonNumericFields(Enum):
    datetime = "Datetime"
    lat = "Lat"
    lon = "Lon"
    station_id = "Station ID"
    station_zone = "Station Zone"
    sea_temp_depth = "Sea Temperature Depth (m)"
    baro_elevation = "Barometer Elevation (m)"
    air_temp_height = "Air T° Height (m)"
    year = "Year"
    month = "Month"
    day = "Day"
    hour = "Hour"
    day_of_week = "DayOfWeek"
    day_period = "DayPeriod"