from imports import *

Base = declarative_base()

def convert_df_columns(df):
    """
    Convertit chaque colonne en son type approprié sans modifier les données
    ou introduire des NaN.
    
    Args:
    - df: pd.DataFrame. Le DataFrame à traiter.
    
    Returns:
    - pd.DataFrame: Le DataFrame avec les types de données convertis.
    """
    
    # Traitement des colonnes avec les types appropriés
    for col in df.columns:
        # Convertir les colonnes numériques
        if df[col].dtype == 'object':
            # Tenter de convertir en float si c'est un nombre représenté par des strings
            try:
                # Convertir en float pour les colonnes qui peuvent l'être (ex: "Wind Speed (km/h)", "Pressure (hPa)", etc.)
                df[col] = pd.to_numeric(df[col], errors='raise')
            except ValueError:
                # Si la conversion échoue, laisser la colonne intacte
                pass
                
        # Convertir des dates si la colonne contient des chaînes de caractères représentant des dates
        if df[col].dtype == 'object' and 'date' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors='raise')
            except ValueError:
                pass
        
        # Convertir les booléens (is_day) en int
        if col == "is_day":
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Assurer les types numériques pour les colonnes déjà numériques mais mal typées
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].astype(pd.Float64Dtype())  # Garantir une gestion correcte des NaN dans les colonnes numériques

    return df

def get_buoy_url(station_id):
            station_id_str = str(station_id)
            url = f"https://www.ndbc.noaa.gov/station_page.php?station={station_id_str}"
            return url

def fetch_table_data(engine, table_name: str) -> pd.DataFrame:
    """
    Récupère les données d'une table MySQL dans une DataFrame Pandas.
    
    Params:
        engine: SQLAlchemy engine pour la connexion MySQL.
        table_name (str): Nom de la table à récupérer.
    
    Returns:
        pd.DataFrame: DataFrame contenant les données de la table.
    """
    try:
        query = text(f"SELECT * FROM `{table_name}`;")
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)

        print(f"✅ Données récupérées depuis '{table_name}' ({len(df)} lignes).")
        return df

    except Exception as e:
        print(f"❌ Erreur lors de la récupération de '{table_name}' : {e}")
        return pd.DataFrame()  # Retourne une DataFrame vide en cas d'erreur

def create_table_in_mysql(df: pd.DataFrame, table_name: str, engine):
    # Vérifier si la table existe déjà
    check_table_sql = f"SHOW TABLES LIKE '{table_name}';"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(check_table_sql))
            if result.fetchone():
                print(f"⚠️ La table '{table_name}' existe déjà dans MySQL.")
            else:
                # Définition des colonnes
                column_definitions = []
                for column_name, dtype in df.dtypes.items():
                    if dtype == 'object': 
                        column_type = "VARCHAR(255)"
                    elif dtype == 'int64':  
                        column_type = "INT"
                    elif dtype == 'float64': 
                        column_type = "FLOAT"
                    elif dtype == 'datetime64[ns]': 
                        column_type = "DATETIME"
                    elif dtype == 'datetime64[ns, UTC]': 
                        column_type = "DATETIME"
                    elif dtype == 'timedelta64[ns]':  
                        column_type = "TIME"
                    elif dtype == 'bool':  
                        column_type = "BOOLEAN"
                    elif dtype == 'time64[ns]': 
                        column_type = "TIME"
                    else:
                        column_type = "VARCHAR(255)" 

                    column_definitions.append(f"`{column_name}` {column_type}")

                # Création de la table
                create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)});"
                connection.execute(text(create_table_sql))
                print(f"✅ Table '{table_name}' créée avec succès dans MySQL.")
            
            # Charger les données sans doublons
            # Vérifier les doublons
            for index, row in df.iterrows():
                # Créer une requête d'insertion
                insert_sql = f"INSERT INTO `{table_name}` ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))});"

                # Vérifier si la ligne existe déjà dans la table
                check_duplicate_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE "
                check_duplicate_conditions = []
                for column in df.columns:
                    check_duplicate_conditions.append(f"`{column}` = %s")
                check_duplicate_sql += " AND ".join(check_duplicate_conditions)

                # Exécuter la vérification des doublons
                duplicate_check_result = connection.execute(text(check_duplicate_sql), tuple(row))
                duplicate_count = duplicate_check_result.fetchone()[0]

                # Si la ligne n'existe pas déjà, l'insérer
                if duplicate_count == 0:
                    connection.execute(text(insert_sql), tuple(row))
                    print(f"✅ Ligne insérée dans la table '{table_name}'.")
                else:
                    print(f"⚠️ Doublon détecté pour la ligne : {row}. Aucune insertion effectuée.")

    except Exception as e:
        print(f"❌ Erreur lors de la création de la table : {e}")

def insert_new_rows(engine, df: pd.DataFrame, table_name: str):
    """
    Insère uniquement les nouvelles lignes de df dans MySQL en comparant la colonne 'Datetime'.
    """
    try:
        # Vérifier si la table existe et récupérer la valeur max de Datetime
        query = f"SELECT MAX(Datetime) FROM `{table_name}`;"
        with engine.connect() as connection:
            result = connection.execute(text(query)).scalar()  # Récupérer la valeur max
        
        # Si la table est vide, insérer tout le DataFrame
        if result is None:
            print(f"💾 Aucune donnée dans '{table_name}', insertion de toutes les lignes.")
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f"✅ {len(df)} nouvelles lignes insérées dans '{table_name}'.")
            return
        
        # Filtrer les nouvelles lignes (celles avec un Datetime plus grand que la valeur max en base)
        df["Datetime"] = pd.to_datetime(df["Datetime"])  # S'assurer que la colonne est bien en datetime
        new_rows = df[df["Datetime"] > result]

        if new_rows.empty:
            print(f"✅ Aucune nouvelle ligne à insérer dans '{table_name}'.")
            return

        # Insérer uniquement les nouvelles lignes
        new_rows.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"✅ {len(new_rows)} nouvelles lignes insérées dans '{table_name}'.")

    except Exception as e:
        print(f"❌ Erreur lors de l'insertion des nouvelles lignes : {e}")

def show_null_counts(df):
    row_count = df.shape[0]
    null_counts = df.isnull().sum()
    
    formatted_output = "\n".join(
        f"{col:<40}{count:<4}/ {row_count}" for col, count in null_counts.items()
    )
    
    print(formatted_output)

def drop_columns_if_exist(df, columns_to_drop):
    print(f" Nombre initial de colonnes: {len(df.columns)}")  # Afficher les colonnes existantes
    existing_columns = []
    for col in columns_to_drop:
        if col in df.columns:
            existing_columns.append(col)
            print(f"Colonne '{col}' Supprimée")
        else: 
            print(f"Colonne '{col}' Non Trouvée")
    
    # Si des colonnes à supprimer existent, les supprimer
    if existing_columns:
        df = df.drop(columns=existing_columns)
    
    print(f" Nombre final de colonnes: {len(df.columns)}")
    return df

def convert_to_datetime(date_value):
    try:
        # Si l'entrée est déjà un objet datetime, on le retourne directement
        if isinstance(date_value, datetime):
            return date_value
        
        # Si l'entrée est un objet pandas.Timestamp, on le convertit en datetime
        if isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime()
        
        # Si l'entrée est une chaîne de caractères, on tente de la convertir en datetime
        if isinstance(date_value, str):
            return datetime.fromisoformat(date_value)
        
    except ValueError as e:
        # Si la conversion échoue, on retourne la valeur d'origine (sans la modifier)
        return date_value  # Retourne la valeur d'origine sans la modifier

def handle_null_values(df):
    # Calcul du nombre de lignes du DataFrame
    num_rows = len(df)

    # Attribution du tag et seuil en fonction du nombre de lignes
    if num_rows > 100000:
        tag = 'green'  # Vert pour les DataFrames de plus de 100 000 lignes
        threshold = 70  # Plus souple pour les DataFrames verts (jusqu'à 70% de données manquantes)
    elif 10000 < num_rows <= 100000:
        tag = 'yellow'  # Jaune pour les DataFrames entre 10 000 et 100 000 lignes
        threshold = 60  # Seuil intermédiaire pour les DataFrames jaunes (jusqu'à 60% de données manquantes)
    elif 2000 < num_rows <= 10000:
        tag = 'orange'  # Orange pour les DataFrames entre 2 000 et 10 000 lignes
        threshold = 55  # Seuil modéré pour les DataFrames entre 2K et 10K (jusqu'à 55% de données manquantes)
    else:
        tag = 'red'  # Rouge pour les DataFrames de moins de 2 000 lignes
        threshold = 50  # Plus strict pour les DataFrames rouges (jusqu'à 50% de données manquantes)

    print(f"\nTag: {tag} - Nombre de lignes: {num_rows}")

    # Calcul du pourcentage de valeurs manquantes par colonne
    missing_percent = (df.isnull().sum() / len(df)) * 100

    # Gestion des valeurs manquantes en fonction du tag
    for column in df.columns:
        null_percentage = missing_percent[column]  # Calcul du pourcentage de valeurs manquantes

        if null_percentage == 0:
            print(f"Colonne '{column}' non modifiée (0% de valeurs manquantes)")
            continue  # Skip this column if there are no missing values

        if null_percentage > threshold:  # Si + de X% de valeurs manquantes (dépend du tag)
            df = df.drop(columns=[column]) 
            print(f"Colonne '{column}' Supprimée (plus de {threshold}% de valeurs manquantes)")

        else:  # Si moins de X% de valeurs manquantes
            if pd.api.types.is_numeric_dtype(df[column]):
                median_value = df[column].median()
                df[column] = df[column].fillna(median_value)  # Imputation par la médiane pour les colonnes numériques
                print(f"Colonne '{column}' Imputée par la médiane ({round(null_percentage,2)}% de valeurs manquantes)")

            else:
                mode_value = df[column].mode()[0]  # Imputation par le mode pour les colonnes non numériques
                df[column] = df[column].fillna(mode_value)
                print(f"Colonne '{column}' Imputée par le mode ({round(null_percentage,2)}% de valeurs manquantes)")

    return df

def meteo_api_request(coordinates, mode='historical', days=92, interval='hourly'):
    """
    Fonction pour récupérer les données météo depuis l'API Open-Meteo avec cache et réessayer en cas d'erreur.
    
    Paramètres:
        coordinates (list) : liste de coordonnées [latitude, longitude]
        mode (str) : intervalle de données ('forecast' ou 'historical', par défaut 'historical')
        days (int) : nombre de jours dans le passé ou dans le futur (par défaut : 92 pour historique)
        interval (str) : intervalle des données ('hourly' ou 'daily', par défaut 'hourly')

    Retourne :
        pd.DataFrame : un DataFrame avec les données météo
    """
    # Fonction utilitaire pour convertir les coordonnées avec ou sans suffixe (ex: '45.5W', '-45.5')
    def parse_coordinates(coord):
        # Vérifie si la coordonnée a un suffixe de direction (W, E, N, S)
        pattern = r"^([-+]?\d+(\.\d+)?)([NSEW]?)$"
        match = re.match(pattern, str(coord))
        if match:
            value = float(match.group(1))
            direction = match.group(3)
            
            if direction == 'W' or direction == 'S':
                value = -abs(value)  # Si direction est Ouest ou Sud, on inverse la valeur
            
            return value
        else:
            raise ValueError(f"Coordonnée invalide : {coord}")

    # Convertir les coordonnées
    latitude = parse_coordinates(coordinates[0])
    longitude = parse_coordinates(coordinates[1])

    # Setup de l'API client avec retry et cache
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)

    url = "https://api.open-meteo.com/v1/forecast"
    
    # Paramètres de base
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": days if mode == 'historical' else None,  # Si historique, utiliser 'past_days'
        "forecast_days": days if mode == 'forecast' else None,  # Si forecast, utiliser 'forecast_days'
        "hourly" if interval.lower() == 'hourly' else "daily": [
            "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "showers", 
            "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", 
            "cloud_cover_high", "visibility", "wind_speed_10m", "soil_temperature_0cm", "soil_moisture_0_to_1cm", 
            "is_day"
        ]
    }

    # Faire l'appel API
    responses = openmeteo.weather_api(url, params=params)

    # Traiter la réponse pour le premier emplacement
    response = responses[0]  # On prend la première réponse si plusieurs lieux sont fournis

    # Initialisation du dictionnaire pour les données à retourner
    data = {}

    # Processus des données en fonction du mode sélectionné
    if mode == 'historical':
        # Traitement pour données historiques
        if interval == 'hourly':
            hourly = response.Hourly()
            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }
            hourly_variables = [
                "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "showers", 
                "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", 
                "cloud_cover_high", "visibility", "wind_speed_10m", "soil_temperature_0cm", "soil_moisture_0_to_1cm", 
                "is_day"
            ]
            
            for i, var in enumerate(hourly_variables):
                hourly_data[var] = [round(value, 2) for value in hourly.Variables(i).ValuesAsNumpy()]

            return pd.DataFrame(hourly_data)

        elif interval == 'daily':
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )
            }
            daily_variables = [
                "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", 
                "sunrise", "sunset", "daylight_duration", "sunshine_duration", "uv_index_max", "uv_index_clear_sky_max", 
                "precipitation_sum", "rain_sum", "showers_sum", "precipitation_hours", "precipitation_probability_max", 
                "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"
            ]
            
            for i, var in enumerate(daily_variables):
                daily_data[var] = [round(value, 2) for value in daily.Variables(i).ValuesAsNumpy()]

            return pd.DataFrame(daily_data)

    # If Forecast is chosen
    elif mode == 'forecast':
        # Traitement pour prévisions
        if interval == 'hourly':
            hourly = response.Hourly()
            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }
            hourly_variables = [
                "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "showers", 
                "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", 
                "cloud_cover_high", "visibility", "wind_speed_10m", "soil_temperature_0cm", "soil_moisture_0_to_1cm", 
                "is_day"
            ]
            
            for i, var in enumerate(hourly_variables):
                hourly_data[var] = [round(value, 2) for value in hourly.Variables(i).ValuesAsNumpy()]

            return pd.DataFrame(hourly_data)

        elif interval == 'daily':
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )
            }
            daily_variables = [
                "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", 
                "sunrise", "sunset", "daylight_duration", "sunshine_duration", "uv_index_max", "uv_index_clear_sky_max", 
                "precipitation_sum", "rain_sum", "showers_sum", "precipitation_hours", "precipitation_probability_max", 
                "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"
            ]
            
            for i, var in enumerate(daily_variables):
                daily_data[var] = [round(value, 2) for value in daily.Variables(i).ValuesAsNumpy()]

            return pd.DataFrame(daily_data)

def get_station_metadata(station_id):
    return api.station(station_id=station_id)

def parse_buoy_json(buoy_metadata):
    print("\n🔍 Début du parsing de la bouée...")

    # Vérifier la présence des clés requises
    if not isinstance(buoy_metadata, dict):
        raise ValueError("❌ Les données fournies ne sont pas un dictionnaire valide.")

    if 'Name' not in buoy_metadata or 'Location' not in buoy_metadata:
        raise ValueError("❌ Les clés 'Name' et 'Location' doivent être présentes dans les données.")

    Name = buoy_metadata['Name']

    # Trouver tout ce qui vient après le premier tiret
    name_parts = Name.split(' - ', 2)
    station_zone = name_parts[1].strip().lower() if len(name_parts) > 1 else "Unknown"
    print(f"🌍 Zone de la station : {station_zone}")

    # Extraction de l'ID
    name_split = Name.split()
    station_id = name_split[1] if len(name_split) > 1 else "Unknown"
    print(f"🆔 Station ID : {station_id}")

    # Extraction des coordonnées depuis "Location"
    location_parts = buoy_metadata["Location"].split()

    if len(location_parts) < 4:
        raise ValueError("❌ Format de 'Location' invalide (au moins 4 éléments attendus).")

    try:
        lat_buoy = f"{float(location_parts[0]):.2f}{location_parts[1]}"
        lon_buoy = f"{float(location_parts[2]):.2f}{location_parts[3]}"
        print(f"✅ Coordonnées extraites : Latitude = {lat_buoy}, Longitude = {lon_buoy}")
    except ValueError:
        raise ValueError("❌ Impossible de convertir les coordonnées en nombres.")

    # Extraire les autres valeurs avec des valeurs par défaut si non présentes
    Water_depth = buoy_metadata.get("Water depth", "N/A")
    print(f"🌊 Water Depth : {Water_depth}")
    
    sea_temp_depth = clean_numeric(buoy_metadata.get("Sea temp depth"))
    print(f"🌡️ Sea Temp Depth : {sea_temp_depth}")
    
    Barometer_elevation = clean_numeric(buoy_metadata.get("Barometer elevation"))
    print(f"🌬️ Barometer Elevation (m): {Barometer_elevation}")
    
    Anemometer_height = clean_numeric(buoy_metadata.get("Anemometer height"))
    print(f"💨 Anemometer Height : {Anemometer_height}")
    
    Air_temp_height = clean_numeric(buoy_metadata.get("Air temp height"))
    print(f"🌤️ Air Temp Height : {Air_temp_height}")

    # Récupérer l'URL de la bouée
    url = get_buoy_url(station_id)
    print(f"🔗 URL de la bouée : {url}")

    # Création du dictionnaire final
    data = {
        "station_zone": station_zone,
        "lat_buoy": lat_buoy,
        "lon_buoy": lon_buoy,
        "Water_depth": Water_depth,
        "sea_temp_depth": sea_temp_depth,
        "Barometer_elevation": Barometer_elevation,
        "Anemometer_height": Anemometer_height,
        "Air_temp_height": Air_temp_height,
        "url": url
    }

    print("✅ Parsing terminé !\n")
    return data

def extract_lat_lon_from_station_list(location):

    # Expression régulière pour capturer la latitude et la longitude
    lat_match = re.search(r'([+-]?\d+\.\d+|\d+)([NS])', location)
    lon_match = re.search(r'([+-]?\d+\.\d+|\d+)([EW])', location)
    
    if lat_match and lon_match:
        # Extraction des valeurs
        lat = float(lat_match.group(1))
        lon = float(lon_match.group(1))
        
        # Inverser la direction de la latitude et longitude si nécessaire
        if lat_match.group(2) == 'S':  # Si la latitude est au Sud
            lat = -lat
        if lon_match.group(2) == 'W':  # Si la longitude est à l'Ouest
            lon = -lon
        
        lat = round(lat, 2)
        lon = round(lon, 2)
        return lat, lon
    return None, None

def print_with_flush(message):

    sys.stdout.write(f'\r{message}  ')  # \r permet de revenir au début de la ligne
    sys.stdout.flush()  # Force l'affichage immédiat

# Extraction des valeurs avec sécurité
def safe_get(dico, key):
    value = dico.get(key, "N/A")  # Valeur par défaut si clé absente
    print(f"📊 {key} : {value}")
    return value

# 🔹 Nettoyer les valeurs numériques (supprimer tout sauf chiffres et ".")
def clean_numeric(value):
    if value is None:
        return None  # ⚠️ Évite de faire `.replace()` sur None !
    return re.sub(r"[^\d.]", "", value).strip()  # Supprime tout sauf chiffres et "."

def convert_coordinates(lat, lon):
    # Conversion de la latitude
    lat_value = float(lat[:-1])  # On enlève la lettre 'n' ou 's' et on garde la valeur numérique
    if lon[-1].lower() == 's':  # Si la latitude est dans l'hémisphère sud
        lat_value = -lat_value

    # Conversion de la longitude
    lon_value = float(lon[:-1])  # On enlève la lettre 'e' ou 'w' et on garde la valeur numérique
    if lon[-1].lower() == 'w':  # Si la longitude est dans l'hémisphère ouest
        lon_value = -lon_value

    return round(lat_value, 2), round(lon_value, 2)

def count_files_in_directory(output_dir):
    try:
        # Vérifier si le dossier existe
        if not os.path.exists(output_dir):
            print(f"Le dossier {output_dir} n'existe pas.")
            return
        
        # Liste des fichiers dans le dossier
        files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))]
        
        # Si le dossier est vide
        if not files:
            print(f"Aucun fichier trouvé dans le dossier {output_dir}.")
            return
        
        print(f"Nombre de fichiers dans le dossier '{output_dir}': {len(files)}\n")
        
        # Analyser chaque fichier
        for file in files:
            file_path = os.path.join(output_dir, file)
            file_name, file_extension = os.path.splitext(file)
            
            # Vérifier si c'est un fichier CSV
            if file_extension.lower() == '.csv':
                try:
                    # Lire le fichier CSV avec pandas
                    df = pd.read_csv(file_path)
                    num_rows, num_cols = df.shape
                    
                    # Afficher les informations sur le fichier
                    print(f"Nom du fichier: {file_name}")
                    print(f"Format: {file_extension}")
                    print(f"Nombre de lignes: {num_rows}, Nombre de colonnes: {num_cols}\n")
                except Exception as e:
                    print(f"Erreur lors de la lecture du fichier {file}: {e}")
            else:
                print(f"Fichier {file} n'est pas un fichier CSV.\n")
    
    except Exception as e:
        print(f"Erreur dans la fonction count_files_in_directory: {e}")

def process_datetime_column(df, column):
    # Convertir la colonne en chaîne de caractères au tout début
    df[column] = df[column].astype(str)
    print(f"📌 La colonne '{column}' est maintenant convertie en chaîne de caractères.")

    # Essayer de convertir la colonne en datetime en forçant la conversion avec errors='coerce'
    try:
        df[column] = pd.to_datetime(df[column], errors='coerce', utc=True)
        print(f"📌 Conversion réussie de '{column}' en datetime.")

        # Renommer la colonne AVANT d'appliquer floor()
        df.rename(columns={column: 'Datetime'}, inplace=True)
        print('✅ Successfully renamed column to "Datetime"!')

        # Appliquer l'arrondi sur la nouvelle colonne renommée et supprimer le fuseau horaire
        df['Datetime'] = df['Datetime'].dt.floor('H').dt.tz_localize(None)
        
    except Exception as e:
        print(f"🚨 ERREUR lors de la conversion de '{column}' : {e}")
    
    return df  # Toujours retourner le DataFrame modifié

def clean_dataframe(df, cols_to_convert, verbose=False):

    # Faire une copie du DataFrame pour éviter les modifications sur une vue
    df = df.copy()

    # Supprimer les colonnes 100% vides
    df.dropna(axis=1, how="all", inplace=True)

    # Conversion des colonnes en float
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
            # Remplacement des NaN par des valeurs adaptées
            df.fillna(df.median(numeric_only=True), inplace=True)

    # Cas spécifiques : visibilité et températures
    if "visibility" in df.columns:
        df["visibility"].fillna(df["visibility"].mean(), inplace=True)

    if "water_level_above_mean" in df.columns:
        df["water_level_above_mean"].fillna(0, inplace=True)

    if "air_temperature" in df.columns:
        df["air_temperature"].fillna(df["air_temperature"].median(), inplace=True)

    if "water_temperature" in df.columns:
        df["water_temperature"].fillna(df["water_temperature"].median(), inplace=True)

    # Vérification finale
    if verbose:
        if df.isnull().sum().sum() > 0:
            print("⚠️ Il reste encore des NaN !")
        else:
            print("✅ Toutes les valeurs ont été remplacées avec succès !")
        print(df.dtypes)

    return df

def convert_columns_to_numeric(df, cols_to_convert):


    df = df.copy()  # Ne pas modifier l'original

    print("Columns before conversion:", df.columns)

    for col in cols_to_convert:
        if col in df.columns:
            try:
                # Essayer de convertir la colonne en numérique
                df[col] = pd.to_numeric(df[col], errors="raise")  # 'raise' lève une erreur si la conversion échoue
            except ValueError:
                print(f"⚠️ Impossible de convertir la colonne {col}, elle reste inchangée.")

    # Si 'is_day' existe, convertissons-le en entier proprement
    if "is_day" in df.columns:
        try:
            df["is_day"] = pd.to_numeric(df["is_day"], errors="raise").astype(int)
        except ValueError:
            print("⚠️ Impossible de convertir 'is_day' en entier, elle reste inchangée.")

    print("Columns after conversion:", df.columns)
    return df

def display_row_values(df, columns=None):
    # Si aucune colonne n'est spécifiée, afficher toutes les colonnes
    if columns is None:
        columns = df.columns.tolist()

    if isinstance(columns, str):
        columns = [columns]
    # Calculer la largeur maximale pour chaque colonne spécifiée
    column_widths = [max(df[col].astype(str).apply(len).max(), len(col)) for col in columns]

    # Afficher les noms des colonnes, en tenant compte des largeurs maximales
    column_headers = [col.ljust(column_widths[i]) for i, col in enumerate(columns)]
    print("  |  ".join(column_headers))
    print("-" * (sum(column_widths) + (len(columns) - 1) * 4))  # Ligne de séparation

    # Afficher les 10 premières valeurs sous chaque colonne spécifiée, alignées
    for i in range(min(10, len(df))):  # Affiche jusqu'à 10 lignes ou le nombre de lignes disponibles
        row_values = [str(df.iloc[i, df.columns.get_loc(col)]).ljust(column_widths[j]) for j, col in enumerate(columns)]
        print("  |  ".join(row_values))

def rename_columns(df, columns):
    # Assurez-vous que l'entrée est bien un DataFrame
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")
    
    # Si `columns` est un dictionnaire, le convertir en une liste de dictionnaires pour un traitement uniforme
    if isinstance(columns, dict):
        columns = [columns]  # Convertir en liste de dictionnaires pour uniformité
    
    # Parcourir chaque dictionnaire de renommage
    if isinstance(columns, list):
        for rename_dict in columns:
            # Créer un dictionnaire de colonnes existantes à renommer
            existing_columns = {col: rename_dict[col] for col in rename_dict if col in df.columns}
            
            if existing_columns:
                # Renommer les colonnes si elles existent dans le DataFrame
                for old_name, new_name in existing_columns.items():
                    print(f"🔄 Colonne '{old_name}' renommée en '{new_name}'")
                
                df.rename(columns=existing_columns, inplace=True)
                print(f"✅ Colonnes renommées : {existing_columns}")
            else:
                print(f"⚠️ Aucune colonne à renommer pour ce dictionnaire : {rename_dict}")
    
    return df

def calculate_closer_probabilities(df, columns):
    """
    Calcule les probabilités que chaque colonne d'un DataFrame soit la plus proche des autres,
    selon l'idée de la "wisdom of the crowd".

    :param df: DataFrame contenant les données.
    :param columns: Liste des noms des colonnes à comparer entre elles.
    :return: Dictionnaire des probabilités pour chaque colonne.
    """
    
    # Créer un dictionnaire pour stocker les résultats des distances
    distances = {}
    
    # Calculer les distances entre chaque paire de colonnes
    for i, col1 in enumerate(columns):
        for j, col2 in enumerate(columns):
            if i < j:  # Calculer les distances uniquement une fois pour chaque paire
                dist_column = f'dist_{col1}_{col2}'
                distances[dist_column] = np.abs(df[col1] - df[col2])
    
    # Ajouter les distances au DataFrame
    for dist_column, values in distances.items():
        df[dist_column] = values
    
    # Créer un dictionnaire pour stocker si une colonne est plus proche des autres
    closer = {col: [] for col in columns}
    
    # Pour chaque ligne, déterminer quelle colonne est la plus proche des autres
    for _, row in df.iterrows():
        for col1 in columns:
            # Trouver les distances entre la colonne 'col1' et les autres
            dist_to_others = [row[f'dist_{col1}_{col2}'] for col2 in columns if col1 != col2]
            # Vérifier si 'col1' est la plus proche de toutes les autres colonnes
            if row[f'dist_{col1}_{col2}'] == min(dist_to_others):
                closer[col1].append(True)
            else:
                closer[col1].append(False)
    
    # Calculer les probabilités pour chaque colonne d'être la plus proche
    probabilities = {col: np.mean(closer[col]).round(3) for col in columns}
    
    return probabilities

def add_hour_day_period_and_month(df, datetime_column='Datetime'):
    """
    Cette fonction ajoute trois colonnes :
    - 'Day Period' : Moment de la journée ('Morning', 'Afternoon', 'Evening', 'Night')
    - 'Month' : Le mois basé sur la colonne 'Datetime'
    - 'Hour' : L'heure extraite de la colonne 'Datetime'
    
    Parameters:
    df (pandas.DataFrame): Le DataFrame contenant la colonne 'Datetime'
    datetime_column (str): Le nom de la colonne contenant les informations de date et d'heure. Par défaut, 'Datetime'
    
    Returns:
    pandas.DataFrame: Le DataFrame avec les nouvelles colonnes ajoutées
    """
    
    # Assurez-vous que la colonne 'Datetime' est bien de type datetime
    df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
    
    # Vérifier si la colonne 'Day Period' existe déjà, sinon l'ajouter
    if 'DayPeriod' not in df.columns:
        # Ajouter la colonne 'Day Period' (Moment de la journée)
        def get_day_period(hour):
            if 6 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 18:
                return 'Afternoon'
            elif 18 <= hour < 22:
                return 'Evening'
            else:
                return 'Night'
        
        df['DayPeriod'] = df[datetime_column].dt.hour.apply(get_day_period)
    
    # Vérifier si la colonne 'Month' existe déjà, sinon l'ajouter
    if 'Month' not in df.columns:
        df['Month'] = df[datetime_column].dt.month
    
    # Vérifier si la colonne 'Hour' existe déjà, sinon l'ajouter
    if 'Hour' not in df.columns:
        df['Hour'] = df[datetime_column].dt.hour
    
    return df

def create_table_in_mysql(df: pd.DataFrame, table_name: str, engine):
    # Vérifier si la table existe déjà
    check_table_sql = f"SHOW TABLES LIKE '{table_name}';"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(check_table_sql))
            if result.fetchone():
                print(f"⚠️ La table '{table_name}' existe déjà dans MySQL.")
            else:
                # Définition des colonnes
                column_definitions = []
                for column_name, dtype in df.dtypes.items():
                    if dtype == 'object': 
                        column_type = "VARCHAR(255)"
                    elif dtype == 'int64':  
                        column_type = "INT"
                    elif dtype == 'float64': 
                        column_type = "FLOAT"
                    elif dtype == 'datetime64[ns]': 
                        column_type = "DATETIME"
                    elif dtype == 'datetime64[ns, UTC]': 
                        column_type = "DATETIME"
                    elif dtype == 'timedelta64[ns]':  
                        column_type = "TIME"
                    elif dtype == 'bool':  
                        column_type = "BOOLEAN"
                    elif dtype == 'time64[ns]': 
                        column_type = "TIME"
                    else:
                        column_type = "VARCHAR(255)" 

                    column_definitions.append(f"`{column_name}` {column_type}")

                # Création de la table
                create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)});"
                connection.execute(text(create_table_sql))
                print(f"✅ Table '{table_name}' créée avec succès dans MySQL.")
                
    except Exception as e:
        print(f"❌ Erreur lors de la création de la table : {e}")

def afficher_info_bouees_aleatoires(buoy_datas, prefix=None, df_wanted=None):
    """
    Affiche les informations de DataFrames (marine et/ou météo) de deux bouées choisies aléatoirement.
    """
    # Sélection de deux bouées au hasard
    sample_buoy_id_marine = random.choice(list(buoy_datas.keys()))
    sample_buoy_id_meteo = random.choice(list(buoy_datas.keys()))

    # Choix du préfixe (ex: "Cleaned")
    marine_key = f"{prefix} Marine" if prefix else "Marine"
    meteo_key = f"{prefix} Meteo" if prefix else "Meteo"

    sample_marine_df = buoy_datas[sample_buoy_id_marine].get(marine_key)
    sample_meteo_df = buoy_datas[sample_buoy_id_meteo].get(meteo_key)

    df_marine_name = "Marine DataFrame"
    df_meteo_name = "Météo DataFrame"

    if df_wanted is None or df_wanted.lower() == "marine":
        if sample_marine_df is not None and not sample_marine_df.empty:
            print(f"🌊 {df_marine_name} pour {sample_buoy_id_marine}:")
            print(sample_marine_df.info())
        else:
            print(f"⚠️ {df_marine_name} pour {sample_buoy_id_marine} : Aucune donnée")

    if df_wanted is None or df_wanted.lower() == "meteo":
        if sample_meteo_df is not None and not sample_meteo_df.empty:
            print(f"☁️ {df_meteo_name} pour {sample_buoy_id_meteo}:")
            print(sample_meteo_df.info())
        else:
            print(f"⚠️ {df_meteo_name} pour {sample_buoy_id_meteo} : Aucune donnée")

def display_buoys_missing_df_counts(buoy_datas, prefix=None, df_wanted=None):
    total = len(buoy_datas)
    marine_empty = 0
    meteo_empty = 0

    marine_key = f"{prefix} Marine" if prefix else "Marine"
    meteo_key = f"{prefix} Meteo" if prefix else "Meteo"

    for buoy_id, data in buoy_datas.items():
        df_marine = data.get(marine_key)
        df_meteo = data.get(meteo_key)

        if (df_wanted is None or df_wanted.lower() == "marine") and (df_marine is None or df_marine.empty):
            marine_empty += 1

        if (df_wanted is None or df_wanted.lower() == "meteo") and (df_meteo is None or df_meteo.empty):
            meteo_empty += 1

    if df_wanted is None or df_wanted.lower() == "marine":
        print(f"\n🌊 Nombre de bouées sans données '{marine_key}' : {marine_empty}/{total}")

    if df_wanted is None or df_wanted.lower() == "meteo":
        print(f"\n☁️ Nombre de bouées sans données '{meteo_key}' : {meteo_empty}/{total}")

def insert_new_rows(df: pd.DataFrame, engine, table: str, ref_column: str):
    """
    Insère les nouvelles lignes dans la table MySQL après avoir vérifié si les IDs uniques existent déjà.

    Args:
    - df (pd.DataFrame): Le DataFrame contenant les nouvelles données.
    - engine (SQLAlchemy Engine): L'engine SQLAlchemy pour se connecter à la base de données.
    - table (str): Le nom de la table dans laquelle insérer les données.
    - ref_column (str): Le nom de la colonne à utiliser comme référence (ID unique).
    """
    try:
        print("🔍 Connexion à la base de données en cours...")

        with engine.connect() as connection:
            # Vérifier si la table est vide
            print("🧮 Vérification si la table est vide...")
            check_empty_sql = f"SELECT COUNT(*) FROM `{table}`"
            result = connection.execute(text(check_empty_sql))
            row_count = result.fetchone()[0]

            if row_count == 0:
                # La table est vide, insérer toutes les données avec compteur
                print("⚡ La table est vide. Insertion de toutes les données avec suivi...")
                total = len(df)
                inserted = 0
                for _, row in df.iterrows():
                    row_df = pd.DataFrame([row])
                    row_df.to_sql(table, con=engine, if_exists='append', index=False)
                    inserted += 1
                    sys.stdout.write(f"\r⏳ Insertion en cours : {inserted}/{total}")
                    sys.stdout.flush()
                print("\n✅ Toutes les lignes ont été insérées dans la table.")
            else:
                # Sinon, récupérer les IDs existants dans la table
                print(f"🔎 Récupération des {ref_column} existants dans la table...")
                check_existing_ids_sql = f"SELECT `{ref_column}` FROM `{table}`"
                result = connection.execute(text(check_existing_ids_sql))
                existing_ids = {row[0] for row in result.fetchall()}
                print(f"🧑‍💻 {len(existing_ids)} {ref_column} existants ont été trouvés dans la table.")

                # Filtrage des lignes du DataFrame
                print("🔄 Filtrage des nouvelles lignes à insérer...")
                new_rows_df = df[~df[ref_column].isin(existing_ids)]

                if not new_rows_df.empty:
                    total = len(new_rows_df)
                    print(f"🚀 Insertion de {total} nouvelles lignes...")
                    inserted = 0
                    for _, row in new_rows_df.iterrows():
                        row_df = pd.DataFrame([row])
                        row_df.to_sql(table, con=engine, if_exists='append', index=False)
                        inserted += 1
                        sys.stdout.write(f"\r⏳ Insertion en cours : {inserted}/{total}")
                        sys.stdout.flush()
                    print("\n✅ Insertion terminée avec succès.")
                else:
                    print(f"⚠️ Aucune nouvelle ligne à insérer, tous les {ref_column} sont déjà présents.")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion dans la table '{table}': {e}")
        
def create_unique_id(df, columns):

    """
    Crée un identifiant unique basé sur des colonnes spécifiées,
    avec une gestion spéciale pour les colonnes de type datetime.
    
    Args:
    df (pandas.DataFrame): DataFrame sur lequel opérer.
    columns (list): Liste des colonnes à utiliser pour créer l'ID unique.
    
    Returns:
    pandas.DataFrame: DataFrame avec un identifiant unique créé et ordonné.
    """
    
    def process_datetime_column(col):
        """
        Formate une colonne datetime en une chaîne d'entiers avec année, mois, jour, heure, minute, etc.
        """
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return df[col].apply(lambda x: f"{x.year:04d}{x.month:02d}{x.day:02d}{x.hour:02d}{x.minute:02d}{x.second:02d}")
        return df[col].astype(str)

    # Créer une liste de colonnes transformées
    unique_columns = []
    
    # Parcourir les colonnes spécifiées
    for col in columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):  # Si la colonne est de type datetime
            unique_columns.append(process_datetime_column(col))  # Applique le format datetime
        else:
            unique_columns.append(df[col].astype(str))  # Sinon, traite comme une chaîne de caractères
    
    # Fusionner toutes les colonnes pour créer l'ID unique
    df['Unique ID'] = pd.concat(unique_columns, axis=1).agg(''.join, axis=1)

    # Placer la colonne 'Unique ID' en première position
    df = df[['Unique ID'] + [col for col in df.columns if col != 'Unique ID']]

    return df

