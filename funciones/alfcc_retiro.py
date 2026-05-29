import sys 
import pandas as pd
from sqlalchemy import create_engine

filename_retiro_telef='blacklist_celulares.txt'



port_mysql = '3306'
user_valentina = "flazo"
pwd_valentina = "T4rg3t2026$$"
server_valentina = "db.mastermold.dev"
db_valentina = "crm_target"

engine_mysql = create_engine(
    f"mysql+pymysql://{user_valentina}:{pwd_valentina}@{server_valentina}:{port_mysql}/{db_valentina}"
)

# traer ventas del mes 
query = """
select dni as col_01 from crm_target.alfcc_ventas
where fecha>='2026-05-01'
"""

df_venta = pd.read_sql(query, engine_mysql)



# evaluar numero enriquecidos

query = """
SELECT  NUMERO_DOCUMENTO as col_01, cl_telf8, cl_telf9, cl_telf10 
FROM crm_target.alfcc_clientes
WHERE cl_base = 'mayo 2026'
and cl_estado=1
"""

df_dni = pd.read_sql(query, engine_mysql)

cols_tel = [
    'cl_telf8','cl_telf9','cl_telf10'
]

df_long = df_dni.melt(
    id_vars='col_01',
    value_vars=cols_tel,
    var_name='tipo_telf',
    value_name='CELULAR'
)
df_long['CELULAR'] = (
    df_long['CELULAR']
    .fillna(0)            
    .astype('int64')        
    .astype(str)              
)
df_long = df_long[
    (df_long['CELULAR'].notna()) &
    (df_long['CELULAR'] != '') &
    (df_long['CELULAR'].str.len() == 9) &
    (df_long['CELULAR'].str.startswith('9'))
]

filePath = os.path.join(ruta_csv, filename_retiro_telef)

df_list = pd.read_csv(filePath)
df_list['CELULAR'] = (
    df_list['CELULAR']
    .fillna(0)            
    .astype('int64')        
    .astype(str)              
)

df_list['CELULAR'] = df_list['CELULAR'].astype(str)

df_list = df_list.merge(
    df_long,
    on="CELULAR",
    how="inner"
)

ids_quitar = df_venta['col_01'].drop_duplicates()

df_list = df_list[
    ~df_list['col_01'].isin(ids_quitar)
]

df_list = df_list['col_01']

from sqlalchemy import text

query = """
DELETE FROM crm_target.tb_temporal
"""

with engine_mysql.begin() as conn:
    result = conn.execute(text(query))
    print("Filas eliminadas:", result.rowcount)


df_list[['col_01']].to_sql(
    name="tb_temporal",
    con=engine_mysql,
    if_exists="append",
    index=False,
    chunksize=1000
)

from sqlalchemy import text

query = """
UPDATE crm_target.alfcc_clientes a
INNER JOIN crm_target.tb_temporal b
    ON a.NUMERO_DOCUMENTO = b.col_01 
SET 
    a.cl_telf8 ='0',
    a.cl_telf9 = '0',
    a.cl_telf10 = '0'
WHERE 
    a.cl_base = 'mayo 2026'
"""

    'cl_telf8','cl_telf9','cl_telf10'


with engine_mysql.begin() as conn:
    result = conn.execute(text(query))
    print("Filas afectadas:", result.rowcount)


reemplazo = {
    'NUMERO_DOCUMENTO': 'col_01',
}
df_list = df_list.rename(columns=reemplazo)

df_list['col_02'] ='Retiro Telf'
df_list['col_03'] ='3'
df_list .head()