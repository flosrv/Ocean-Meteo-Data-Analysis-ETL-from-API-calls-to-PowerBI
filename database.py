from functions import *

table_dim_station = "dim_station"
table_dim_time = "dim_time"
table_facts_meteo = "facts_meteo"
table_facts_ocean = "facts_ocean"
db_staging = 'db_staging'
db_DW ='oceanography_data_analysis'

engine_staging = create_mysql_engine(db_name=db_staging)
engine_DW = create_mysql_engine(db_name=db_DW)