import sys 
sys.path.append('C:/Users/DATA/Documents/datos/01_script/inicio/funciones')
from funciones import *
from variables_inicio import *

params_sa = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server_sa};"
    f"DATABASE={db_sa};"
    f"UID={user_sa};"
    f"PWD={pwd_sa}"
)


params_kishin = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server_kishin};"
    f"DATABASE={db_kishin};"
    f"UID={user_kishin};"
    f"PWD={pwd_kishin}"
)


params_zeus = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server_zeus};"
    f"DATABASE={db_zeus};"
    f"UID={user_zeus};"
    f"PWD={pwd_zeus}"
)


def get_data_sql(query,params_sa):
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_sa}")
    return pd.read_sql(query, engine)


    