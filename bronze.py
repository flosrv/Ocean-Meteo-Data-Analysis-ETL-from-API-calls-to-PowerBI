from imports import *
from functions import *

# get all stations and some metadata as a Pandas DataFrame
stations_df = api.stations()
stations_df = api.stations(as_df=True)

# Liste de mots à rechercher dans la colonne "Remark"
blacklist = ["Failure", "ceased", "failed", "recovered", "stopped", 'adrift']
stations_id_set = set()

# Liste pour collecter les indices à supprimer
indices_a_supprimer = []

# Liste des URL où il y a des erreurs d'accès
access_error_url_list = []

# Parcours des lignes de la DataFrame
for idx, row in stations_df.iterrows():
    station_id = row["Station"]
    station_Location = row["Hull No./Config and Location"]  # Extraire la valeur de la cellule pour chaque ligne
    
    # Extraction du nom de la station si un ")" est trouvé
    try:
        if ")" in station_Location:
            station_name = station_Location.split(')')[1].rstrip(" )")  # On enlève l'espace et la parenthèse en fin de chaîne
        else:
            station_name = station_Location.strip()  # Si pas de ")", on garde toute la chaîne

        station_name = station_name.rstrip(" )").replace("(", "").replace(")", "").strip()

        # Nettoyage final pour enlever toute parenthèse ou espace en fin de nom
        station_name = station_name.rstrip(" )")

    except Exception as e:
        print(f"❌ Error processing station name for {station_id}: {e}")
        continue  # Continuer avec la station suivante en cas d'erreur

    # Vérifier si "Remark" n'est pas NaN et si un des éléments de blacklist est dans "Remark"
    try:
        if isinstance(row["Remark"], str) and any(blacklist_word.lower() in row["Remark"].lower() for blacklist_word in blacklist):
            indices_a_supprimer.append(idx)
        else:
            try:
                # Effectuer l'appel API
                buoy_data = NDBC.realtime_observations(station_id)
                
                # Vérifier si les données de l'API sont valides (si le DataFrame n'est pas vide)
                if not buoy_data.empty:
                    print(f'Buoy {station_id}: {station_name} passed the Remarks and API Test!')
                    stations_id_set.add(station_id)
                else:
                    print(f'Buoy {station_id}: {station_name} did not return valid data. Deleting.')
                    indices_a_supprimer.append(idx)

            except Exception as e:
                # Gestion des erreurs d'API
                if isinstance(e, HTTPError):
                    print(f'Buoy {station_id}: {station_name} API Call returned {e.code}. Deleting.')
                else:
                    print(f'Buoy {station_id}: {station_name} API Call encountered an error. Deleting.')
                    
                    if str(e).startswith("Error accessing"):
                        url = f"https://www.ndbc.noaa.gov/station_page.php?station={station_id}"
                        access_error_url_list.append([station_id, url])
                indices_a_supprimer.append(idx)

    except Exception as e:
        print(f"❌ Error processing remarks for station {station_id}: {e}")
        indices_a_supprimer.append(idx)  # Ajouter l'index à supprimer en cas d'erreur

# Supprimer les lignes après la boucle
try:
    stations_df.drop(index=indices_a_supprimer, inplace=True)
except Exception as e:
    print(f"❌ Error while dropping indices: {e}")

######## BIG API CALL LOOP

dict_df = {}

# Définir le dossier où sauvegarder les fichiers
output_dir = r"bronze_tables"
# S'assurer que le dossier existe (le créer s'il n'existe pas)
os.makedirs(output_dir, exist_ok=True)

buoy_datas = {}

# Boucle pour récupérer les données de chaque bouée
for buoy_id in stations_id_set:  # Boucle sur les bouées validées
    try:
        buoy_datas[buoy_id] = {}
        
        # Récupérer les métadonnées de la bouée
        metadata_extracted = get_station_metadata(buoy_id)
        station_id, station_zone, lat_buoy, lon_buoy, data_table_name = parse_buoy_json(metadata_extracted)
        
        buoy_datas[buoy_id]["Station Name"] = station_name
        buoy_datas[buoy_id]["Station ID"] = station_id
        buoy_datas[buoy_id]["Zone"] = station_zone
        buoy_datas[buoy_id]["Lat"] = lat_buoy
        buoy_datas[buoy_id]["Lon"] = lon_buoy
        Bronze_Marine_Table_Name = buoy_datas[buoy_id]["Marine Table Name"] = "bronze_tables/marine_data_" + data_table_name
        Bronze_Meteo_Table_Name = buoy_datas[buoy_id]["Meteo Table Name"] = "bronze_tables/meteo_data_" + data_table_name
        
        marine_csv_path = os.path.join(output_dir, "marine_data_" + data_table_name + ".csv")
        meteo_csv_path = os.path.join(output_dir, "meteo_data_" + data_table_name + ".csv")

        duo = [Bronze_Marine_Table_Name, Bronze_Meteo_Table_Name]

        # NOAA API CALL
        try:
            df_marine = NDBC.realtime_observations(buoy_id)
            
            if df_marine is None or df_marine.empty:
                print(f"⚠️ Marine Data is empty for buoy {buoy_id}")
            else: 
                df_marine["Station ID"] = buoy_id
                buoy_datas[buoy_id]["Marine Dataframe"] = df_marine
                print(f"🌊 Marine Data Successfully collected for buoy {buoy_id}")
                buoy_datas[buoy_id]["Marine DataFrame"] = df_marine
                df_marine.to_csv(marine_csv_path, mode='w', index=True, index_label="time")
        
        except Exception as e:
            print(f"❌ Failed to collect Marine Data for buoy {buoy_id}: \n{e}")

        # Open-Meteo API Call 
        try:
            df_meteo = meteo_api_request(coordinates=[lat_buoy, lon_buoy])
            
            if df_meteo is None or df_meteo.empty:
                print(f"⚠️ Meteo Data is empty for buoy {buoy_id}")
            else:
                buoy_datas[buoy_id]["Meteo DataFrame"] = df_meteo
                print(f"🌦️ Meteo Data Successfully collected for buoy {buoy_id}")
                buoy_datas[buoy_id]["Meteo DataFrame"] = df_meteo
                df_meteo.to_csv(meteo_csv_path, mode='w', index=True, index_label="date")
        
        except Exception as e:
            print(f"⚠️ Bouée {buoy_id} : Erreur à l'insertion des données meteo:\n{e}\n")

    except Exception as e:
        print(f"❌ Error processing buoy {buoy_id}: {e}")

if os.path.exists(output_dir):
    # Liste tous les fichiers dans le répertoire
    for file_name in os.listdir(output_dir):
        # Vérifier si c'est un fichier CSV
        if file_name.endswith('.csv'):
            # Construire le chemin complet du fichier
            file_path = os.path.join(output_dir, file_name)
            
            try:
                # Lire le fichier CSV avec pandas
                df = pd.read_csv(file_path)
                
                # Afficher le nom du fichier et le nombre de lignes
                print(f"{file_name}: {len(df)} rows")
            except Exception as e:
                print(f"❌ Error reading {file_name}: {e}")
else:
    print(f"❌ The directory {output_dir} does not exist.")


print("Script Ended Successfully !")
###########################################################################################################################
