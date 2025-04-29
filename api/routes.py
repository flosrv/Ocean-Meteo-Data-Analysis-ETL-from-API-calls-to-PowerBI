from imports import *


router = APIRouter()

def get_session():
    # Crée une session pour interagir avec la base de données
    with Session(engine_DW) as session:
        yield session

# ==============================
# Routes pour DimStation
# ==============================

@router.get("/station/{station_id}")
async def get_station_data(
    station_id: str,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer les données d'une station donnée.
    L'ID de la station est utilisé pour effectuer la recherche.
    """
    # Recherche des données de la station dans la base de données
    station_data = session.exec(select(DimStation).where(DimStation.station_id == station_id)).first()

    # Si aucune donnée n'est trouvée pour cette station, on renvoie une erreur 404
    if not station_data:
        raise HTTPException(status_code=404, detail="Station not found")
    
    # Retourne les données de la station
    return station_data

# ==============================
# Routes pour DimTime
# ==============================

@router.get("/time/{datetime_value}")
async def get_time_data(
    datetime_value: datetime,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer les données pour un moment donné (Datetime).
    La valeur de datetime est utilisée pour effectuer la recherche.
    """
    # Recherche des données pour la date donnée dans la table DimTime
    time_data = session.exec(select(DimTime).where(DimTime.datetime_value == datetime_value)).first()

    # Si aucune donnée n'est trouvée pour ce moment donné, on renvoie une erreur 404
    if not time_data:
        raise HTTPException(status_code=404, detail="Time not found")
    
    # Retourne les données pour ce moment précis
    return time_data

# ==============================
# Routes pour FactsMeteo
# ==============================

@router.get("/meteo/{station_id}")
async def get_meteo_data(
    station_id: str,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer toutes les données météorologiques pour une station donnée.
    L'ID de la station est utilisé pour effectuer la recherche.
    """
    # Recherche des données météorologiques pour la station donnée
    meteo_data = session.exec(select(FactsMeteo).where(FactsMeteo.station_id == station_id)).all()

    # Si aucune donnée n'est trouvée pour cette station, on renvoie une erreur 404
    if not meteo_data:
        raise HTTPException(status_code=404, detail="No meteo data found for this station")
    
    # Retourne toutes les données météorologiques pour la station donnée
    return meteo_data

@router.get("/meteo/{station_id}/date/{date}")
async def get_meteo_data_by_date(
    station_id: str,
    date: datetime,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer les données météorologiques pour une station à une date spécifique.
    La station et la date sont utilisées pour effectuer la recherche.
    """
    # Recherche des données météorologiques pour la station et la date spécifiques
    meteo_data = session.exec(select(FactsMeteo).where(
        FactsMeteo.station_id == station_id,
        FactsMeteo.datetime_value == date
    )).first()

    # Si aucune donnée n'est trouvée, on renvoie une erreur 404
    if not meteo_data:
        raise HTTPException(status_code=404, detail="No meteo data found for this station on this date")
    
    # Retourne les données météorologiques pour cette station à la date donnée
    return meteo_data

# ==============================
# Calcul de la Moyenne Mobile sur 7 Jours pour les données météos
# ==============================

@router.get("/meteo/{station_id}/moving_avg/7days/{date}")
async def get_moving_avg_7days_meteo(
    station_id: str,
    date: datetime,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de calculer la moyenne mobile des températures pour les 7 derniers jours,
    à partir de la date donnée.
    """
    # Détermine la date de début (7 jours avant la date donnée)
    start_date = date - timedelta(days=6)
    
    # Recherche des données météorologiques entre la date de début et la date donnée
    meteo_data = session.exec(select(FactsMeteo).join(DimTime, FactsMeteo.datetime_value == DimTime.datetime_value)
                              .where(FactsMeteo.station_id == station_id)
                              .where(DimTime.datetime_value.between(start_date, date))).all()

    # Si aucune donnée n'est trouvée dans la plage de dates, on renvoie une erreur 404
    if not meteo_data:
        raise HTTPException(status_code=404, detail="No meteo data found for this station in the given date range")

    # Calcul de la moyenne des températures sur la période donnée (7 jours)
    avg_temp = sum([data.temperature_c for data in meteo_data if data.temperature_c is not None]) / len(meteo_data)
    
    # Retourne la moyenne des températures pour la station et la période donnée
    return {
        "station_id": station_id,
        "date": date,
        "7_day_moving_avg_temperature": avg_temp
    }

# ==============================
# Routes pour FactsOcean
# ==============================

@router.get("/ocean/{station_id}")
async def get_ocean_data(
    station_id: str,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer toutes les données océaniques pour une station donnée.
    L'ID de la station est utilisé pour effectuer la recherche.
    """
    # Recherche des données océaniques pour la station donnée
    ocean_data = session.exec(select(FactsOcean).where(FactsOcean.station_id == station_id)).all()

    # Si aucune donnée n'est trouvée pour cette station, on renvoie une erreur 404
    if not ocean_data:
        raise HTTPException(status_code=404, detail="No ocean data found for this station")
    
    # Retourne toutes les données océaniques pour la station donnée
    return ocean_data

@router.get("/ocean/{station_id}/date/{date}")
async def get_ocean_data_by_date(
    station_id: str,
    date: datetime,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de récupérer les données océaniques pour une station à une date spécifique.
    La station et la date sont utilisées pour effectuer la recherche.
    """
    # Recherche des données océaniques pour la station et la date spécifiées
    ocean_data = session.exec(select(FactsOcean).where(
        FactsOcean.station_id == station_id,
        FactsOcean.datetime_value == date
    )).first()

    # Si aucune donnée n'est trouvée, on renvoie une erreur 404
    if not ocean_data:
        raise HTTPException(status_code=404, detail="No ocean data found for this station on this date")
    
    # Retourne les données océaniques pour cette station à la date donnée
    return ocean_data

# ==============================
# Calcul de la Moyenne Mobile sur 7 Jours pour les données océaniques
# ==============================

@router.get("/ocean/{station_id}/moving_avg/7days/{date}")
async def get_moving_avg_7days_ocean(
    station_id: str,
    date: datetime,
    session: Session = Depends(get_session)
):
    """
    Cette route permet de calculer la moyenne mobile de la hauteur des vagues pour les 7 derniers jours,
    à partir de la date donnée.
    """
    # Détermine la date de début (7 jours avant la date donnée)
    start_date = date - timedelta(days=6)

    # Recherche des données océaniques entre la date de début et la date donnée
    ocean_data = session.exec(select(FactsOcean).join(DimTime, FactsOcean.datetime_value == DimTime.datetime_value)
                              .where(FactsOcean.station_id == station_id)
                              .where(DimTime.datetime_value.between(start_date, date))).all()

    # Si aucune donnée n'est trouvée dans la plage de dates, on renvoie une erreur 404
    if not ocean_data:
        raise HTTPException(status_code=404, detail="No ocean data found for this station in the given date range")

    # Calcul de la moyenne de la hauteur des vagues sur la période donnée (7 jours)
    avg_wave_height = sum([data.wave_height for data in ocean_data if data.wave_height is not None]) / len(ocean_data)
    
    # Retourne la moyenne de la hauteur des vagues pour la station et la période donnée
    return {
        "station_id": station_id,
        "date": date,
        "7_day_moving_avg_wave_height": avg_wave_height
    }
