from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import engine_staging, cleaned_meteo_data, get_db_staging
from sqlalchemy import func, asc
from api.models import MeteoNumericFields, MeteoNonNumericFields  
from functions import *
from imports import *
import numpy as np
from fastapi.params import Query

router = APIRouter()

# Route pour obtenir la médiane d'une colonne choisie dans cleaned_meteo_data
@router.get("/median")
async def get_median(
    column: MeteoNumericFields,
    station_id: str = Query(None, description="ID de la station à filtrer"),
    db: Session = Depends(get_db_staging)
):
    """
    Calcule la médiane pour une colonne numérique spécifiée du jeu de données nettoyé
    en utilisant les dates minimales et maximales disponibles dans la table.
    Le filtre peut être appliqué par 'Station ID' si fourni.
    """
    column_name = column.value

    if column_name not in cleaned_meteo_data.c.keys():
        raise HTTPException(status_code=400, detail=f"Colonne '{column_name}' non trouvée.")

    column_obj = cleaned_meteo_data.c[column_name]

    # Construire la requête avec un filtre par Station ID si fourni
    query = select(column_obj).where(cleaned_meteo_data.c[column_name] != None)
    
    if station_id:
        query = query.where(cleaned_meteo_data.c["Station ID"] == station_id)

    # Récupérer les données
    with db.begin():
        result = db.execute(query).scalars().all()

    if not result:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible pour calculer la médiane.")

    # Utiliser numpy pour calculer la médiane
    median_value = np.median(result)

    # Récupérer la date minimale et maximale
    min_date = db.execute(select(func.min(cleaned_meteo_data.c["Datetime"]))).scalar()
    max_date = db.execute(select(func.max(cleaned_meteo_data.c["Datetime"]))).scalar()

    return {
        "column": column_name,
        "median_value": median_value,
        "min_date": min_date.strftime("%Y-%m-%d"),
        "max_date": max_date.strftime("%Y-%m-%d")
    }

# Route pour obtenir la médiane mobile sur 7 jours
@router.get("/median/moving7")
async def get_moving_median_7_days(
    date: datetime = Query(..., description="Date au format YYYY-MM-DD"),
    column: MeteoNumericFields = Query(..., description="Colonne numérique pour la médiane mobile"),
    station_id: str = Query(None, description="ID de la station à filtrer"),
    db: Session = Depends(get_db_staging)
):
    """
    Calcule la médiane mobile sur 7 jours (de d-7 à d) pour une colonne numérique spécifique.
    Le filtre peut être appliqué par 'Station ID' si fourni.
    """
    column_name = column.value
    end_date = date
    start_date = end_date - timedelta(days=7)

    if column_name not in cleaned_meteo_data.c.keys():
        raise HTTPException(status_code=400, detail=f"Colonne '{column_name}' non trouvée.")

    column_obj = cleaned_meteo_data.c[column_name]

    # Construire la requête avec un filtre par Station ID si fourni
    query = select(column_obj).where(cleaned_meteo_data.c["Datetime"] >= start_date)
    query = query.where(cleaned_meteo_data.c["Datetime"] <= end_date)
    
    if station_id:
        query = query.where(cleaned_meteo_data.c["Station ID"] == station_id)

    # Récupérer les données pour la période sélectionnée
    with db.begin():
        result = db.execute(query).scalars().all()

    if not result:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible pour calculer la médiane.")

    # Calculer la médiane avec numpy
    median_value = np.median(result)

    return {
        "column": column_name,
        "median_value": median_value,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }

@router.get("/{column}/records_compared_to_threshold")
async def get_records_in_range(
    column: MeteoNumericFields,
    greater_than: Optional[float] = Query(None, description="Seuil de valeur minimale (égal ou supérieur à)"),
    less_than: Optional[float] = Query(None, description="Seuil de valeur maximale (égal ou inférieur à)"),
    station_id: Optional[str] = Query(None, description="Filtrer par ID de station"),
    start_date: Optional[str] = Query(None, description="Date de début YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Date de fin YYYY-MM-DD"),
    db: Session = Depends(lambda: engine_staging.connect())
):
    """
    Renvoie les enregistrements où la colonne sélectionnée est dans les seuils fournis (supérieur et/ou inférieur).
    Permet de filtrer par station ID, date de début et date de fin.
    """
    
    if not (greater_than or less_than):
        return {"error": "Au moins un des paramètres `greater_than` ou `less_than` doit être spécifié."}
    
    filters = []
    
    if greater_than is not None:
        filters.append(cleaned_meteo_data.c[column.value] > greater_than)
    
    if less_than is not None:
        filters.append(cleaned_meteo_data.c[column.value] < less_than)

    if station_id:
        filters.append(cleaned_meteo_data.c[MeteoNonNumericFields.station_id.value] == station_id)

    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            filters.append(cleaned_meteo_data.c[MeteoNonNumericFields.datetime.value] >= start)
        except ValueError:
            return {"error": "start_date invalide (attendu: YYYY-MM-DD)"}

    if end_date:
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            filters.append(cleaned_meteo_data.c[MeteoNonNumericFields.datetime.value] <= end)
        except ValueError:
            return {"error": "end_date invalide (attendu: YYYY-MM-DD)"}

    query = db.execute(
        select(cleaned_meteo_data).where(and_(*filters))
    )

    results = []
    for row in query.fetchall():
        record = dict(row._mapping)
        dt_raw = record.get(MeteoNonNumericFields.datetime.value)
        if isinstance(dt_raw, datetime):
            record[MeteoNonNumericFields.datetime.value] = dt_raw.strftime("%Y-%m-%d %H:%M")
        results.append(record)

    return {
        "filter_applied": {
            "column": column.name,
            "greater_than": greater_than,
            "less_than": less_than,
            "station_id": station_id,
            "start_date": start_date,
            "end_date": end_date
        },
        "records_count": len(results),
        "records": results
    }
