import sys 
sys.path.append('C:/Users/DATA/Documents/datos/01_script/inicio/funciones')
from funciones import *
from funciones_spark import *
from variables_inicio import *
from utils_sql import *


from sqlalchemy import create_engine


engine_mysql = create_engine(
    f"mysql+pymysql://{user_valentina}:{pwd_valentina}@{server_valentina}:{port_mysql}/{db_valentina}"
)

query = """
SELECT  NUMERO_DOCUMENTO,cl_telf1, cl_telf2, cl_telf3, cl_telf4, cl_telf5, cl_telf6, cl_telf7, cl_telf8, cl_telf9, cl_telf10, cl_movil, cl_celular, cl_telefono FROM crm_target.alfin_clientes
WHERE cl_base = 'mayo 2026'
and cl_estado=1
"""

df_dni = pd.read_sql(query, engine_mysql)

cols_tel = [
    'cl_telf1','cl_telf2','cl_telf3','cl_telf4','cl_telf5',
    'cl_telf6','cl_telf7','cl_telf8','cl_telf9','cl_telf10',
    'cl_movil','cl_celular','cl_telefono'
]

df_long = df_dni.melt(
    id_vars='NUMERO_DOCUMENTO',
    value_vars=cols_tel,
    var_name='tipo_telf',
    value_name='TELEFONO'
)
df_long['TELEFONO'] = (
    df_long['TELEFONO']
    .fillna(0)            
    .astype('int64')        
    .astype(str)              
)
df_long = df_long[
    (df_long['TELEFONO'].notna()) &
    (df_long['TELEFONO'] != '') &
    (df_long['TELEFONO'].str.len() == 9) &
    (df_long['TELEFONO'].str.startswith('9'))
]

filename='RetiroDeGestion_Telefonos.csv'
filePath = os.path.join(ruta_csv, filename)
df_list = pd.read_csv(filePath)
df_list['TELEFONO'] = (
    df_list['TELEFONO']
    .fillna(0)            
    .astype('int64')        
    .astype(str)              
)