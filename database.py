from imports import *

# Charger les informations de connexion MySQL
path_to_mysql_creds = r"c:\Credentials\mysql_creds.json"
with open(path_to_mysql_creds, 'r') as file:
    content = json.load(file)
    mysql_user = content["user"]
    password = content["password"]
    host = content["host"]
    port = content["port"]
    database = "oceanography_data_analysis"
    

metadata = MetaData()
# Connexion à MySQL avec SQLModel
engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{password}@{host}:{port}/{database}", isolation_level='AUTOCOMMIT')
Session = sessionmaker(bind=engine)
session = Session()



