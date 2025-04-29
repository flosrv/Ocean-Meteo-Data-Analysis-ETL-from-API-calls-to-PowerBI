from fastapi import FastAPI, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
from database import cleaned_marine_data, cleaned_meteo_data, engine_staging


app = FastAPI()

# ========= ROUTES MARINE ===========

@app.get("/marine/avg_wave_height")
def avg_wave_height(station_id: Optional[str] = None):
    with engine_staging.connect() as conn:
        query = select(func.avg(cleaned_marine_data.c.wave_height))
        if station_id:
            query = query.where(cleaned_marine_data.c.station_id == station_id)
        result = conn.execute(query).scalar()
        return {"avg_wave_height": result}


@app.get("/marine/max_water_temp")
def max_water_temperature():
    with engine_staging.connect() as conn:
        result = conn.execute(
            select(func.max(cleaned_marine_data.c.water_temp))
        ).scalar()
        return {"max_water_temp": result}


@app.get("/marine/wave_stats")
def wave_stats(station_id: Optional[str] = None):
    with engine_staging.connect() as conn:
        query = select(
            func.min(cleaned_marine_data.c.wave_height).label("min"),
            func.avg(cleaned_marine_data.c.wave_height).label("avg"),
            func.max(cleaned_marine_data.c.wave_height).label("max")
        )
        if station_id:
            query = query.where(cleaned_marine_data.c.station_id == station_id)
        result = conn.execute(query).mappings().first()
        return result


# ========= ROUTES METEO ===========

@app.get("/meteo/avg_temperature")
def avg_temperature(station_id: Optional[str] = None):
    with engine_staging.connect() as conn:
        query = select(func.avg(cleaned_meteo_data.c.temperature_c))
        if station_id:
            query = query.where(cleaned_meteo_data.c.station_id == station_id)
        result = conn.execute(query).scalar()
        return {"avg_temperature": result}


@app.get("/meteo/precipitations_by_month")
def precipitations_by_month(year: str = Query(...)):
    with engine_staging.connect() as conn:
        query = select(
            cleaned_meteo_data.c.month,
            func.sum(cleaned_meteo_data.c.precipitations).label("total_precip")
        ).where(cleaned_meteo_data.c.year == year).group_by(cleaned_meteo_data.c.month)
        result = conn.execute(query).mappings().all()
        return result


@app.get("/meteo/extreme_conditions")
def extreme_conditions():
    with engine_staging.connect() as conn:
        query = select(
            cleaned_meteo_data.c.datetime,
            cleaned_meteo_data.c.temperature_c,
            cleaned_meteo_data.c.wind_gusts,
            cleaned_meteo_data.c.precipitations
        ).where(
            (cleaned_meteo_data.c.temperature_c > 35) |
            (cleaned_meteo_data.c.wind_gusts > 80) |
            (cleaned_meteo_data.c.precipitations > 50)
        )
        result = conn.execute(query).mappings().all()
        return result
