import os
import chardet
import re
import pandas as pd
import calendar

import findspark
findspark.init()
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,TimestampType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions
from datetime import *
from pyspark.sql import Window

# Variables
ruta_base = 'D:\\datos'
ruta_archivo_generado = "C:\\Users\\DATA\\Documents\\datos\\generado"

hostname = 'e45129156'
port = '1433'
username='sa'
password='123456'

import gc
import pyodbc
from sqlalchemy import create_engine, MetaData, Table, delete, and_
import shutil
import math
import calendar


import unicodedata


# variables
# ----------------------------------------------------------------------------------------------------

ruta_base = 'D:\\datos'

headers_dic = {
    'migras': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'movil', 'rut', 'nombre_base_cargada', 'idSocialMedia', 'lista', 'recarga_3', 'recarga_2', 'recarga_1', 'prom_rec_3', 'plan', 'rango_rec', 'q_registros', 'plan_sugerido_1', 'plan_sugerido_2', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'migras_v2': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'FECHA_CARGA', 'TIPIFICACION', 'Movil', 'Rut', 'Nombre_Base_Cargada', 'Nombre', 'Comuna', 'Compania', 'idSocialMedia', 'decil_propension', 'PLAN', 'percentil', 'PLAN_SUGERIDO', 'Q_REGISTROS', 'Tipo_Base', 'CL_ADICIONAL1', 'CL_ADICIONAL2', 'CL_ADICIONAL3', 'CL_ADICIONAL4', 'CL_ADICIONAL5'],
    'migras3': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'movil', 'rut', 'nombre_base_cargada', 'lista', 'recarga_3', 'recarga_2', 'recarga_1', 'prom_rec_3', 'plan', 'rango_rec', 'q_registros', 'plan_sugerido_1', 'plan_sugerido_2', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'segundas': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'movil', 'rut', 'nombre_base_cargada', 'nombre', 'comuna', 'oferta', 'cant_lineas_rut', 'desc_plan', 'direccion_comercial', 'decil_juntos', 'decil_cambio_equipo', 'lista', 'codigo_equipo_1', 'codigo_equipo_2', 'codigo_equipo_3', 'equipo_sugerido_1', 'equipo_sugerido_2', 'equipo_sugerido_3', 'plan_sugerido', 'q_registros', 'cf_sugerido', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'porta': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'movil', 'nombre_base_cargada', 'comuna', 'compania', 'q_registros', 'tipo_base', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'recuperados': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'nombre_base_cargada', 'compania', 'descr_movil', 'desc_cliente', 'id_rutcliente', 'q_registros', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'ivr': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'fecha_carga', 'tipificacion', 'movil', 'nombre_base_cargada', 'comuna', 'compania', 'q_registros', 'tipo_base', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5'],
    'emp_fijo': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'tipificacion', 'fecha_carga_base', 'nombre_base_cargada', 'desc_cliente', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5', 'altura_final', 'calle_final', 'comuna_final', 'movil_2', 'movil_3', 'movil_4', 'movil_5', 'movil_administrador', 'nombre_administrador', 'region_profundidad', 'rut_cliente_dv', 'rut_num', 'movil_1'],
    'emp_laser': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'tipificacion', 'movil', 'rut', 'fecha_carga_base', 'nombre_base_cargada', 'nombre', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5', 'correo', 'ciudad_part', 'comuna_part', 'region_part'],
    'emp_cross': ['record_id', 'contact_info', 'contact_info_type', 'record_type', 'record_status', 'call_result', 'attempt', 'dial_sched_time', 'call_time', 'daily_from', 'daily_till', 'tz_dbid', 'campaign_id', 'agent_id', 'chain_id', 'chain_n', 'group_id', 'app_id', 'treatments', 'media_ref', 'email_subject', 'email_template_id', 'switch_id', 'tipificacion', 'movil', 'rut', 'fecha_carga_base', 'nombre_base_cargada', 'nombre', 'ciudad', 'comuna', 'q_registros', 'cl_adicional1', 'cl_adicional2', 'cl_adicional3', 'cl_adicional4', 'cl_adicional5', 'region1', 'correo'],
    'emp_fibra_cross': ['record_id','contact_info','contact_info_type','record_type','record_status','call_result','attempt','dial_sched_time','call_time','daily_from','daily_till','tz_dbid','campaign_id','agent_id','chain_id','chain_n','group_id','app_id','treatments','media_ref','email_subject','email_template_id','switch_id','fecha_carga','tipificacion','movil','rut','nombre_base_cargada','nombre','percentil_cambio','percentil_fuera','ganancia','prox_gama','grupo','iphone','q_registros','decil_cambio','decil_fuera','oferta_eq','otro_sugerido_2','otro_sugerido_3','precio_contado','precio_pago_en_boleta','sku_oferta_eq','sku_sugerido_2','sku_sugerido_3','tipo_fin_base','valor_cuota_boleta','valor_cuota_tc','cl_adicional1','cl_adicional2','cl_adicional3','cl_adicional4','cl_adicional5','cluster_1'],
}

# ----------------------------------------------------------------------------------------------------



# Configurar la conexión a SQL Server
nameDB_01 = 'DB_ventas'
cadena_conexion = f'mssql+pyodbc://{username}:{password}@{hostname}/{nameDB_01}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(cadena_conexion)

# Configura tu cadena de conexión a SQL Server
conn_str = f'DRIVER={{SQL Server}};SERVER={hostname};nameDB_01={nameDB_01};UID={username};PWD={password}'

# Configurar la conexión a SQL Server
nameDB_02 = 'DB_A365'
cadena_conexion_02 = f'mssql+pyodbc://{username}:{password}@{hostname}/{nameDB_02}?driver=ODBC+Driver+17+for+SQL+Server'
engine_02 = create_engine(cadena_conexion_02)

# Configura tu cadena de conexión a SQL Server
conn_str_02 = f'DRIVER={{SQL Server}};SERVER={hostname};nameDB_01={nameDB_02};UID={username};PWD={password}'






# funciones sentencias SQL
# ----------------------------------------------------------------------------------------------------
def Export_list_base_sql(df,nameDB,nameTB):
    database = nameDB
    connTable=nameTB
    jdbc_url = f"jdbc:sqlserver://{hostname}:{port};database={database}"
    connProperties={
        "user": f"{username}", 
        "password": f"{password}", 
        "trustServerCertificate": "true",
        "truncate": "true",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        df.write.jdbc(url=jdbc_url, table=connTable, mode='append', properties=connProperties)
    except Exception as e:  
        print("Error al insertar JDBC:", e) 

def overwrite_table_SQL(df,nameDB,nameTB):
    database = nameDB
    connTable=nameTB
    jdbc_url = f"jdbc:sqlserver://{hostname}:{port};database={database}"
    connProperties={
        "user": f"{username}", 
        "password": f"{password}", 
        "trustServerCertificate": "true",
        "truncate": "true",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        df.write.jdbc(url=jdbc_url, table=connTable, mode='overwrite', properties=connProperties)
    except Exception as e:  
        print("Error al insertar JDBC:", e)   

def obtener_tabla_sql(spark,db,query):
    database = db
    connQuery=query
    #connTable='TB_outboundMovil'
    connTable=f"({connQuery}) AS tmp"
    jdbc_url = f"jdbc:sqlserver://{hostname}:{port};database={database}"
    connProperties={
        "user": f"{username}", 
        "password": f"{password}", 
        "trustServerCertificate": "true",
    }
    return spark.read.jdbc(url=jdbc_url, table=connTable, properties=connProperties) 

def csv_spark(spark,db,nombre_archivo,delimitador):
    filePath = os.path.join(ruta_base, nombre_archivo)
    return spark.read.csv(filePath, sep=delimitador, header=True)


# funciones py
# ----------------------------------------------------------------------------------------------------
def mover_archivo(ruta_base,filename):
    filepath = os.path.join(ruta_base, filename)
    file = os.path.splitext(os.path.basename(filename))[0]
    # Ruta destino
    dir_destino = 'D:/impulsa/temporal01'
    # Construir la ruta completa de destino
    ruta_destino = os.path.join(dir_destino, os.path.basename(filepath))
    # Mover el archivo y reemplazar si ya existe
    shutil.move(filepath, ruta_destino)
    print(f"El archivo {file} suhido")

# Obtener archivo para las bases
# ----------------------------------------------------------------------------------------------------

def modificar_archivo(filename,header2):
    header_campana=header2
    filePath = os.path.join(ruta_base, filename)
    if os.path.exists(filePath):
        # filePath .rsl en modo lectura

        with open(filePath, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']
        with open(filePath, 'r', encoding=encoding) as f:
            # Leer todas las líneas del filePath
            lines = f.readlines()
        lines = [line.replace('|', 'z') for line in lines]

        for i, line in enumerate(lines):
            for columna in header_campana:
                lines[i] = re.sub(r'z' + re.escape(columna)+'=', '|'+re.escape(columna)+'=', lines[i], flags=re.IGNORECASE)
        
        # lines = [line.replace('zrecord_id=', 'record_id=') for line in lines]

        with open(filePath, 'w', encoding="utf-8") as f:
            # Escribe las líneas modificadas en el mismo filePath
            f.writelines(lines)
        print(f"Se ha modificado y guardado el archivo {filename}")

def cargar_archivo_csv(spark,filename,sep,bol_header):
    filePath = os.path.join(ruta_base, filename)
    return spark.read.csv(filePath, sep=sep, header=bol_header)


def obtener_archivo_base_campana(spark,filename,header2):
    header_1=header2
    df01=cargar_archivo_csv(spark,filename,'|',False)
    df_vacio = df01.filter("1==0")
    if len(df01.columns) != len(header_1):
        modificar_archivo(filename,header_1)
        df01=cargar_archivo_csv(spark,filename,'|',False)

        if len(df01.columns) != len(header_1):
            print(f"Formato no encontrado para {filename}")
            return df_vacio
            
    df01 = df01.toDF(*header2)
    for column_name  in df01.columns:
        df01 = df01.withColumn(column_name, 
                expr(f"SUBSTRING({column_name}, INSTR({column_name}, '=') + 1)"))

    return df01


# ----------------------------------------------------------------------------------------------------


# Actualizar tabla de tipificaciones
# ----------------------------------------------------------------------------------------------------
def AgregarTipisNuevas(spark,df,nombreDB,filename):
    from pyspark.sql import functions as F

    df_rsl = df.na.drop(subset=["tipificacion"])\
                .filter(col("tipificacion") != "")\
                .withColumn('codigo_genesys', F.lower(F.col('tipificacion')))\
                .select('codigo_genesys')\
                .distinct()

    query1="""
    select 
        id_tipi,codigo_genesys1,lower(codigo_genesys) as codigo_genesys,estado,contacto,interes,calificacion,venta,motivo1,motivo,descripcion1,descripcion,televendible,peso,factibilidad,obs1
    from TB_tipificacionGlobal
    """
    df_tipi=obtener_tabla_sql(spark,'DB_ArbolTipi',query1)

    df_nuevas_tipis =df_rsl.join(df_tipi, ["codigo_genesys"], "leftanti") 
    df_nuevas_tipis=df_nuevas_tipis.dropna()
    df_nuevas_tipis=df_nuevas_tipis.filter(col('codigo_genesys') != '')
    if not df_nuevas_tipis.isEmpty():
        df_tipi_1=df_tipi.filter("1 = 0")
        df_nuevas_tipis_1 = df_tipi_1.join(df_nuevas_tipis, "codigo_genesys", "outer")
        df_nuevas_tipis_2 = df_nuevas_tipis_1.withColumn("estado", when((col("codigo_genesys").contains('contactado')) & (~col("codigo_genesys").contains('no contactado')),'CONTACTADO')
                                                    .when((col("codigo_genesys").contains('conectado')) & (~col("codigo_genesys").contains('no conectado')),'CONTACTADO')
                                                    .when(col("codigo_genesys").contains('no contactado'),'NO CONTACTADO')
                                                    .when(col("codigo_genesys").contains('no conectado'),'NO CONTACTADO')
                                                    .otherwise('NA'))

        df_nuevas_tipis_2 = df_nuevas_tipis_2.withColumn("contacto", when((col("codigo_genesys").contains('-valido')) & (~col("codigo_genesys").contains('-novalido')),'VALIDO')
                                                    .when((col("codigo_genesys").contains('-valido')) & (~col("codigo_genesys").contains('-no valido')),'VALIDO')
                                                    .when(col("codigo_genesys").contains('-novalido'),'NO VALIDO')
                                                    .when(col("codigo_genesys").contains('-no valido'),'NO VALIDO')
                                                    .otherwise('NA'))
        df_nuevas_tipis_2 = df_nuevas_tipis_2.withColumn("interes", when((col("codigo_genesys").contains('interesa')) & (~col("codigo_genesys").contains('-nointeresa')),'INTERESA')
                                                    .when((col("codigo_genesys").contains('-interesa')) & (~col("codigo_genesys").contains('-no interesa-')),'INTERESA')
                                                    .when(col("codigo_genesys").contains('-nointeresa'),'NO INTERESA')
                                                    .when(col("codigo_genesys").contains('-no interesa'),'NO INTERESA')
                                                    .otherwise('NA'))
        df_nuevas_tipis_2 = df_nuevas_tipis_2.withColumn("calificacion", when((col("codigo_genesys").contains('califica')) & (~col("codigo_genesys").contains('-nocalifica')),'CALIFICA')
                                                    .when((col("codigo_genesys").contains('-califica')) & (~col("codigo_genesys").contains('-no califica')),'CALIFICA')
                                                    .when(col("codigo_genesys").contains('-nocalifica'),'NO CALIFICA')
                                                    .when(col("codigo_genesys").contains('-no califica'),'NO CALIFICA')
                                                    .otherwise('NA'))
        df_nuevas_tipis_2 = df_nuevas_tipis_2.withColumn("venta", when(col("codigo_genesys").contains('-venta'),'VENTA')
                                                .otherwise('NA'))
        df_nuevas_tipis = df_nuevas_tipis.withColumn('obs1',lit(f'{filename}'))

        df_nuevas_tipis = df_nuevas_tipis.drop('id_tipi')
        Export_list_base_sql(df_nuevas_tipis,'DB_ArbolTipi','TB_tipificacionGlobal')
        print(f"--> Se añadieron nuevas tipificaciones de {filename}")

# ----------------------------------------------------------------------------------------------------

def add_comuna_key(spark,df):
    query1 = f"""
    select 
    distinct
    id_comuna
    ,comuna_ref as comuna
    from TB_Comuna
    """
    df_comuna = obtener_tabla_sql(spark,'DB_BaseSemanal',query1)
    return df.join(df_comuna, ["comuna"], "left")



# constuir tabla para tb_discado
# ----------------------------------------------------------------------------------------------------   
def print_mensaje2(filename,mensaje):
    print(f"""{mensaje}No tiene registros nuevos para añadir | {filename} 
----------------------------------------------------------------------------------------------------------------------""")

def obtener_fecha_discado(filename):
    match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"

def obtener_list_campanas(df,col_name):
    una_columna= df.select(col_name).distinct().collect()
    return [row[col_name] for row in una_columna]

def construir_tabla_discado(spark,df,nameDB,filename):
    from pyspark.sql import functions as F
    df=df
    fecha_discado=obtener_fecha_discado(filename)

    df_call_time = df.filter(col("call_time").isNotNull())
    
    if df_call_time.isEmpty():
        print(f'call_time nulo | {filename} ')
        print('----------------------------------------------------------------------------------------------------------------------')
        return df_call_time

    mensaje=''
    df_call_time=df[to_date(df['call_time'], 'M/d/yyyy h:mm:ss a')>=fecha_discado]
    # df_call_time = df.filter(col("call_time") >= fecha_discado)
    if df_call_time.isEmpty():
        print(f'No tiene registros gestionados mayor igual al {fecha_discado} | {filename} ')
        print('----------------------------------------------------------------------------------------------------------------------')
        return df_call_time

    formato_fecha_hora_original = "M/d/yyyy h:mm:ss a"
    df = df.withColumn("call_time", to_timestamp("call_time", formato_fecha_hora_original))
    df = df.withColumn("call_time", to_timestamp("call_time", "yyyy-MM-dd HH:mm:ss"))


    if not df.isEmpty():
        df = df.withColumn("dial_sched_time", to_timestamp("dial_sched_time", formato_fecha_hora_original))
        df = df.withColumn("dial_sched_time", to_timestamp("dial_sched_time", "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("contact_info", col("contact_info").cast("bigint"))
        df = df.withColumn("contact_info", substring(col("contact_info"), -8, 8))
        df = df.withColumn("contact_info", col("contact_info").cast("integer"))
        df = df.withColumn('tipificacion', lower('tipificacion'))


        df = df.withColumn("agent_id", when(col("call_time").isNull(),'NA')
                                    .when(F.lower(F.col("agent_id")).contains('rescheduled'),'NA')
                                    .when(col("agent_id")=='','NA')
                                    .when(col("agent_id").isNull(),'NA')
                                    .otherwise(col('agent_id')))
        
        df = df.withColumn("tipificacion", when(col("record_type") == 'No Call', 'no call')
                                .when(col("call_time").isNull(), 'registro nuevo')
                                .when((F.lower(F.col("call_result"))== 'answer') & (col("tipificacion") == ''), 'tipificacion vacia')
                                .when((F.lower(F.col("call_result"))== 'answer') & (col("tipificacion").isNull()), 'tipificacion vacia')
                                .when(F.lower(F.col("call_result"))!= 'answer', 'maquina')
                                .otherwise(col("tipificacion")))
        
        query1="""
        select 
            id_tipi
            ,lower(codigo_genesys) as tipificacion
        from DB_ArbolTipi.dbo.TB_tipificacionGlobal
        """
        df_sql=obtener_tabla_sql(spark,'DB_ArbolTipi',query1)

        df = df.join(df_sql, ["tipificacion"], "left") 

        df = df.withColumn("id_tipi", when(col("tipificacion")=='no call',20297)
                                .when(col("tipificacion")=='registro nuevo',20296)
                                .when(col("tipificacion")=='tipificacion vacia',20295)
                                .when(col("tipificacion")=='maquina',20298)
                                .otherwise(col('id_tipi')))

        df = df.withColumn('fecha_discado', to_date(lit(fecha_discado), 'yyyy-MM-dd'))
        
        fecha_maxima = df.select(max("call_time")).collect()[0][0]
        df_fecha_maxima = df.filter(col("call_time") == fecha_maxima)
        hora_maxima = df_fecha_maxima.select(date_format(max("call_time"), "HH:mm")).collect()[0][0]
        df = df.withColumn('hora_ref', lit(hora_maxima))
        df = df.withColumn('obs1', lit('0'))

        df=df.select('fecha_discado','hora_ref','chain_id','contact_info','nombre_base_cargada','attempt','id_tipi','agent_id','record_type','record_status','call_result','dial_sched_time','call_time','daily_from','daily_till','chain_n','obs1')    
    else:
        print_mensaje2(filename,mensaje)
    return df
        
# ----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------

# construir_tabla_campana
def construir_tabla_migras(df):

    df = df.withColumn("id_bloque",when(upper(col("nombre_base_cargada")).contains('MIGRACIONES2'),10)
                        .when(upper(col("nombre_base_cargada")).contains('MIGRACIONES'),9)
                        .when(upper(col("nombre_base_cargada")).contains('CNTA'),11)
                        )
    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumn('id_compania', lit('0'))
    df = df.withColumn('comuna', lit(''))
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('id_tipoBase', lit('0'))
    return df
    
def construir_tabla_segundas(df):

    df = df.withColumn("id_bloque",when(upper(col("nombre_base_cargada")).like('%_2'),10)
    .when((upper(col("nombre_base_cargada")).like('%_2')) & (col('nombre_base_cargada').contains('LINEAS_ADICIONALES')), 10)
                        .when(upper(col("nombre_base_cargada")).contains('LINEAS_ADICIONALES'),9)
                        )
    
    df = df.withColumn('percentil', col('decil_cambio_equipo'))
    df = df.withColumn('decil', col('decil_juntos'))
    df = df.withColumn('id_compania', lit('0'))
    df = df.withColumn('id_tipoBase', lit('0'))
    
    df = df.withColumn('id_campana2',when((col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES')) & (upper(col('cl_adicional1')).contains('JUNTOS')), 4)
                                    .when((col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES')) & (upper(col('cl_adicional1')).contains('DELTA')), 5)
    )
    
    return df

    # ya tiene rut y comuna

def construir_tabla_porta(df):
    
    df = df.withColumn("id_bloque",when(upper(col("tipo_base"))==1, 9)
                                .when(upper(col("tipo_base"))==2, 10)
                        )
    
    df = df.withColumn("id_compania",when(upper(col("compania"))=='VTR', 1)
                                    .when(upper(col("compania"))=='WOM', 2)
                                    .when(upper(col("compania"))=='CLARO', 3)
                                    .when(upper(col("compania"))=='MOVISTAR', 4)
                                    .when(upper(col("compania"))=='ENTEL', 5)
                        )

    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('rut', lit('NA'))
    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumn('id_tipoBase', lit('1'))
    return df

def construir_tabla_recuperados(df):
    df = df.withColumn("id_bloque",when((col('nombre_base_cargada').contains('RECUPERADOS')) &(upper(col("cl_adicional5"))==1), 9)
                            .when((col('nombre_base_cargada').contains('RECUPERADOS')) &(upper(col("cl_adicional5"))==2), 10)                            
                    )

    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumnRenamed('id_rutcliente','rut')
    df = df.withColumnRenamed('cl_adicional1','comuna')
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))

    df = df.withColumn("id_compania",when(upper(col("compania"))=='VTR', 1)
                                    .when(upper(col("compania"))=='WOM', 2)
                                    .when(upper(col("compania"))=='CLARO', 3)
                                    .when(upper(col("compania"))=='MOVISTAR', 4)
                                    .when(upper(col("compania"))=='ENTEL', 5)
                        )

    df = df.withColumn('id_tipoBase', col('cl_adicional5'))
    return df

def construir_tabla_ivr(df):
    
    df = df.withColumn("id_tipoBase",when(upper(col("tipo_base")) == 'IVR_PP ON', 3)
        .when(upper(col("tipo_base")) == 'IVR_SS', 2)                            
        .when(upper(col("tipo_base")) == 'IVR_PP OFF', 1)
                .otherwise(0)
        )
    
    df = df.withColumn('id_campana2', lit('0'))
    f_inicio=regexp_extract("nombre_base_cargada", r"(\d{8})", 1)
    f_inicio_1= unix_timestamp(f_inicio, "ddMMyyyy")
    f_inicio_2 = from_unixtime(f_inicio_1).cast(DateType())
    
    num_dia_semana = when(dayofweek(f_inicio_2)==1,7).otherwise(dayofweek(f_inicio_2)-1)

    bloque = when(num_dia_semana.isin([1, 2]), 1)\
            .when(num_dia_semana.isin([3, 4]), 2)\
            .when(num_dia_semana == 5, 3)

    df = df.withColumn("id_bloque", bloque)

    df = df.withColumn("fecha_inicio", regexp_extract("nombre_base_cargada", r"(\d{8})", 1))
    df = df.withColumn("fecha_inicio", unix_timestamp(col("fecha_inicio"), "ddMMyyyy"))
    df = df.withColumn("fecha_inicio", from_unixtime(col("fecha_inicio")).cast(DateType()))

    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('rut', lit('NA'))

    df = df.withColumn("id_compania",when(upper(col("compania"))=='VTR', 1)
                                    .when(upper(col("compania"))=='WOM', 2)
                                    .when(upper(col("compania"))=='CLARO', 3)
                                    .when(upper(col("compania"))=='MOVISTAR', 4)
                                    .when(upper(col("compania"))=='ENTEL', 5)
    )
    return df

def construir_tabla_fibra_hogar(df):
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('id_tipoBase', lit('0'))

    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumn('id_bloque', lit('1'))
    # df = df.withColumn('id_detalle', lit('1'))
    df = df.withColumn('id_compania', lit('0'))

    df = df.withColumn('rut', when(col('rut_num').contains('-'), split(df['rut_num'], '-').getItem(0))
                            .otherwise(df['rut_num']))
    df = df.withColumn('dv', when(col('rut_num').contains('-'), split(df['rut_num'], '-').getItem(1))
                        .otherwise(None)) 
    df = df.withColumnRenamed('comuna_final','comuna')
    df = df.withColumnRenamed('desc_cliente','nombre_cliente')
    df = df.withColumnRenamed('nombre_administrador','nombre_admin')
    df = df.withColumnRenamed('calle_final','direccion')
    df = df.withColumnRenamed('cl_adicional1','email')
    
    return df

def construir_tabla_fibra_hogar_laser(df):
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('id_tipoBase', lit('0'))

    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumn('id_bloque', lit('1'))
    # df = df.withColumn('id_detalle', lit('1'))
    df = df.withColumn('id_compania', lit('0'))
    df = df.withColumn('direccion', lit(''))

    df = df.withColumn('dv', when(col('rut').contains('-'), split(df['rut'], '-').getItem(1))
                        .otherwise(None)) 
    df = df.withColumn('rut', when(col('rut').contains('-'), split(df['rut'], '-').getItem(0))
                            .otherwise(df['rut']))
    df = df.withColumnRenamed('comuna_part','comuna')
    df = df.withColumnRenamed('nombre','nombre_cliente')
    df = df.withColumnRenamed('cl_adicional1','nombre_admin')
    df = df.withColumnRenamed('correo','email')
    
    return df

def construir_tabla_fibra_hogar_cross(df):
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('decil', lit('0'))
    df = df.withColumn('id_tipoBase', lit('0'))

    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumn('id_bloque', lit('1'))
    df = df.withColumn('id_compania', lit('0'))
    df = df.withColumn('direccion', lit(''))

    df = df.withColumn('dv', when(col('rut').contains('-'), split(df['rut'], '-').getItem(1))
                        .otherwise(None)) 
    df = df.withColumn('rut', when(col('rut').contains('-'), split(df['rut'], '-').getItem(0))
                            .otherwise(df['rut']))
    df = df.withColumnRenamed('nombre','nombre_cliente')
    df = df.withColumnRenamed('cl_adicional1','nombre_admin')
    df = df.withColumnRenamed('correo','email')
    
    return df

def construir_tabla_fibra_cross(df):

    df = df.withColumn('id_campana2', lit('0'))
    df = df.withColumnRenamed('Decil_Fuera','decil')
    df = df.withColumnRenamed('cl_adicional2','comuna')

    df = df.withColumnRenamed('cl_adicional1','compania')
    df = df.withColumn("id_compania",when(upper(col("compania"))=='VTR', 1)
                                .when(upper(col("compania"))=='WOM', 2)
                                .when(upper(col("compania"))=='CLARO', 3)
                                .when(upper(col("compania"))=='MOVISTAR', 4)
                                .when(upper(col("compania"))=='ENTEL', 5)
    )
    
    df = df.withColumn('id_tipoBase', lit('0'))
    df = df.withColumn('percentil', lit('0'))
    df = df.withColumn('id_bloque', lit('9'))

    return df


def construir_tabla_bases(spark,df,filename):
    df = df.withColumn("nombre_base_cargada", upper(col("nombre_base_cargada")))
    df_ref01=df.select('nombre_base_cargada').distinct()
    df_ref02=obtener_tabla_sql(spark,
        'DB_BaseSemanal',
        "select distinct nombre_base_cargada FROM TB_BaseCargada"
    )

    df_ref01= df_ref01.join(df_ref02, on=["nombre_base_cargada"], how="leftanti")

    if not df_ref01.isEmpty():
        df_ref01 = df_ref01.withColumn('id_campana',when(col('nombre_base_cargada').contains('FIBRA_CROSS3.'),11)
                                                .when(col('nombre_base_cargada').contains('CNTA'), 10)
                                                .when(col('nombre_base_cargada').contains('MIGRACIONES2'), 1)
                                                .when(col('nombre_base_cargada').contains('MIGRACIONES'), 1)
                                                .when(col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES'), 2)
                                                .when(col('nombre_base_cargada').contains('PERFILADA2'), 5)
                                                .when(col('nombre_base_cargada').contains('PERFILADA'), 5)
                                                .when(col('nombre_base_cargada').contains('RECUPERADOS'), 6)
                                                .when(col('nombre_base_cargada').contains('PORTA_IVR_PILOTO'), 7) # relativo
                                                .when(col('nombre_base_cargada').contains('PORTA_IVR'), 7)
                                                .when((col('nombre_base_cargada').contains('EMPRESAS_FIJO')) | (col('nombre_base_cargada').contains('bd_onnet')), 8)
                                                .when(col('nombre_base_cargada').contains('EMPRESAS_CROSS'), 8)
        )

        df_ref01 = df_ref01.withColumn('skill',when(col('nombre_base_cargada').contains('FIBRA_CROSS3.0'), 'CMP_CL_OUT_ECO_A365_RENOVACION_EQUIPOS3')
                                                .when(col('nombre_base_cargada').contains('CNTA'), 'CMP_CL_OUT_ECO_A365_MIGRACIONES3')
                                                .when(col('nombre_base_cargada').contains('MIGRACIONES2'), 'CMP_CL_OUT_ECO_A365_MIGRACIONES2')
                                                .when(col('nombre_base_cargada').contains('MIGRACIONES'), 'CMP_CL_OUT_ECO_A365_MIGRACIONES')
                                                .when(col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES'), 'CMP_CL_OUT_ECO_A365_SEGUNDAS_LINEAS')
                                                .when(col('nombre_base_cargada').contains('PERFILADA2'), 'CMP_CL_OUT_ECO_A365_PORTA_EXCLUSIVA')
                                                .when(col('nombre_base_cargada').contains('PERFILADA'), 'CMP_CL_OUT_ECO_A365_PORTA_PERFILADA')
                                                .when(col('nombre_base_cargada').contains('RECUPERADOS'), 'CMP_CL_OUT_ECO_A365_PORTA_RECUPERADOS')
                                                .when(col('nombre_base_cargada').contains('PORTA_IVR_PILOTO'), 'CMP_CL_OUT_ECO_A365_PORTA_6') # relativo
                                                .when(col('nombre_base_cargada').contains('PORTA_IVR'), 'CMP_CL_OUT_ECO_A365_PORTA_IVR')
                                                .when((col('nombre_base_cargada').contains('EMPRESAS_FIJO')) | (col('nombre_base_cargada').contains('bd_onnet')), 'CMP_CL_OUT_EMP_A365_HOGAR_FIBRA_CLIENTE')
                                                .when(col('nombre_base_cargada').contains('EMPRESAS_CROSS'), 'CMP_CL_OUT_EMP_A365_HOGAR_FIBRA_NO_CLIENTE')
        )

        df_ref01 = df_ref01.withColumn('nombre_skill',when(col('nombre_base_cargada').contains('FIBRA_CROSS3.0'), 'FIBRA_CROSS3')
                                                        .when(col('nombre_base_cargada').contains('CNTA'), 'CONECTA_MAYOR')
                                                        .when(col('nombre_base_cargada').contains('MIGRACIONES2'), 'MIGRACIONES2')
                                                        .when(col('nombre_base_cargada').contains('MIGRACIONES'), 'MIGRACIONES')
                                                        .when(col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES'), 'SEGUNDAS_LINEAS')
                                                        .when(col('nombre_base_cargada').contains('PERFILADA2'), 'PORTA_PERFILADA2')
                                                        .when(col('nombre_base_cargada').contains('PERFILADA'), 'PORTA_PERFILADA')
                                                        .when(col('nombre_base_cargada').contains('RECUPERADOS'), 'PORTA_RECUPERADOS')
                                                        .when(col('nombre_base_cargada').contains('PORTA_IVR_PILOTO'), 'PORTA_IVR_PILOTO') # relativo
                                                        .when(col('nombre_base_cargada').contains('PORTA_IVR'), 'PORTA_IVR')
                                                        .when((col('nombre_base_cargada').contains('EMPRESAS_FIJO')) | (col('nombre_base_cargada').contains('bd_onnet')), 'EMPRESAS_FIJO')
                                                        .when(col('nombre_base_cargada').contains('EMPRESAS_CROSS'), 'EMPRESAS_CROSS')
        )

        fecha_sin_formato=regexp_extract("nombre_base_cargada", r"(\d{8})", 1)
        fecha_con_formato=unix_timestamp(fecha_sin_formato, "ddMMyyyy")
        fecha_con_formato_1=from_unixtime(fecha_con_formato).cast(DateType())

        df_ref01 = df_ref01.withColumn("fecha_carga", fecha_con_formato_1)
        df_ref01 = df_ref01.withColumn("fecha_inicio", col('fecha_carga'))
            
        num_dia_semana = when(dayofweek(col("fecha_inicio"))==1,7).otherwise(dayofweek(col("fecha_inicio"))-1)
        fecha_resultante = col("fecha_inicio") + 7-num_dia_semana
        ultimo_dia_mes_inicio = last_day(col("fecha_inicio"))

        fecha_final = when(
            month(fecha_resultante) > month(col("fecha_inicio")),
            ultimo_dia_mes_inicio
        ).otherwise(fecha_resultante)

        df_ref01 = df_ref01.withColumn("fecha_final", fecha_final)
        
        sem_anio = weekofyear(col("fecha_inicio"))
        sem_anio_primer_dia = weekofyear(trunc(col("fecha_inicio"), "MM"))
        sem_mes = sem_anio - sem_anio_primer_dia + 1
        df_ref01 = df_ref01.withColumn("n_sem", sem_mes)
            
        if 'reciclado' in filename.lower():
            df_ref01 = df_ref01.withColumn("tipo_base", lit('RECICLADO'))

        return df_ref01
    else:
        return df_ref01
    

def add_columnas(spark,filename,df):

    df = df.withColumn('id_campana2',when(col('nombre_base_cargada').contains('FIBRA_CROSS3.'), 18)
                            .when(col('nombre_base_cargada').contains('CNTA'), 3)
                            .when(col('nombre_base_cargada').contains('MIGRACIONES2'), 2)
                            .when(col('nombre_base_cargada').contains('MIGRACIONES'), 1)
                            .when(col('nombre_base_cargada').contains('PERFILADA2'), 16)
                            .when(col('nombre_base_cargada').contains('PERFILADA'), 7)
                            .when(col('nombre_base_cargada').contains('RECUPERADOS'), 8)
                            .when(col('nombre_base_cargada').contains('porta_ivr_piloto'), 17) # relativo
                            .when(col('nombre_base_cargada').contains('PORTA_IVR'), 10)
                            .when((col('nombre_base_cargada').contains('empresas_fijo')) | (col('nombre_base_cargada').contains('bd_onnet')), 11)
                            .when(col('nombre_base_cargada').contains('empresas_cross'), 19)
                            .when(col('nombre_base_cargada').contains('BD_LINEAS_ADICIONALES'), col('id_campana2'))
    )

    if "reciclado" in filename.lower():
        df=df.withColumn('id_detalle',lit(5))
    elif "adicional" in filename.lower():
        df=df.withColumn('id_detalle',lit(2))
    else:
        df=df.withColumn('id_detalle',lit(1))

    df=add_comuna_key(spark,df)
    df_idbaseCargada =df.select('nombre_base_cargada').distinct()
    df_ref=obtener_tabla_sql(spark,
        'DB_BaseSemanal',
        "select distinct nombre_base_cargada,id_baseCargada FROM TB_BaseCargada"
    )

    df_ref01= df_idbaseCargada.join(df_ref,["nombre_base_cargada"],"left")
    
    df=df.join(df_ref01,["nombre_base_cargada"], "left")

    df=df.select('id_baseCargada','chain_id','contact_info','rut','id_campana2','id_compania','id_comuna','id_bloque','percentil', 'decil', 'id_tipoBase','id_detalle')

    df = df.withColumn("contact_info", col("contact_info").cast("bigint"))
    df = df.withColumn("contact_info", substring(col("contact_info"), -8, 8))
    df = df.withColumn("contact_info", col("contact_info").cast("integer"))
    
    df = df.withColumn('rut',when(col('rut').isNull(), 'NA')
                            .when(col('rut')==0, 'NA')
                            .when(col('rut')=='0', 'NA')
                            .otherwise(col('rut'))
                            )

    return df


# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------


def limpiar_texto(text):
    if pd.isna(text):
        return 'NA'
    text = str(text)
    text = text.upper()
    # text = text.replace('NAN', 'NA')
    # Reemplazar caracteres específicos
    reemplazos_especificos = {
        'Ã¡': 'A',
        'Ã©': 'E',
        'Ã­': 'I',
        'Ã³': 'O',
        'Ãº': 'U',
        'Ã±': 'N',
        'ã': 'A',
        'Ã¼': 'U',
        'Ã‘': 'N',
        'Ã±': 'N'
    }
    for orig, repl in reemplazos_especificos.items():
        text = text.replace(orig, repl)
    # Normalizar caracteres Unicode a ASCII
    text = unidecode.unidecode(text)
    # Eliminar texto entre < y >
    text = re.sub(r'<[a-zA-Z0-9]*>', '', text)
    # Eliminar comillas simples y dobles
    text = re.sub(r"[\"']", "", text)
    # Convertir todo el texto a mayúsculas
    
    return text


def quitar_tildes(texto):
    texto_normalizado = unicodedata.normalize('NFD', texto)
    texto_sin_tildes = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')
    return texto_sin_tildes

def add_column_rename_or_create(df, opc1, new_name,resultado):
    
    found = False
    for col in df.columns:
        sin_tilde=quitar_tildes(opc1)
        if sin_tilde in col.lower():
            df = df.rename(columns={col: new_name})
            found = True
            break
    if not found:
        df[new_name] = resultado
    return df

def add_column_rename_or_create_ID(df):
    if 'ID' in df.columns:
        df = df.rename(columns={'ID': 'id'})
    else:
        df['id'] = ''
    return df

def ultimos_8_caracteres(text):
    text = str(text)
    if text is None:
        return None
    return text[-8:]

def reemplazar_text(text):
    text = str(text)
    if text is None:
        return None
    
    if text == 'NAN':
        return 'NA'
    else:
        return text

def construir_tabla(df02,nom_recepcion): 
    df02['recepcion'] = nom_recepcion
    df02.columns = [quitar_tildes(col) for col in df02.columns]
    df02=add_column_rename_or_create(df02,'movil','contact_info','NA')
    df02=add_column_rename_or_create(df02,'rut','rut','NA')
    df02=add_column_rename_or_create(df02,'decil','decil',0)
    df02=add_column_rename_or_create(df02,'percentil','percentil',0)
    df02=add_column_rename_or_create(df02,'plan','id_plan','NA')
    df02=add_column_rename_or_create(df02,'comuna','comuna','NA')
    df02=add_column_rename_or_create(df02,'compan','compania','NA')
    df02=add_column_rename_or_create_ID(df02)
    if nom_recepcion=='segundas_lineas':
        df02['nombre_cliente'] = df02[['NOMBRE1', 'NOMBRE_2', 'APELLIDO_PATERNO', 'APELLIDO_MATERNO']]\
            .apply(lambda x: ' '.join(x.replace('', pd.NA).dropna().astype(str).str.strip()).strip(), axis=1)
    else:
        df02=add_column_rename_or_create(df02,'cliente','nombre_cliente','NA')

    if nom_recepcion=='porta_ivr':
        df02=add_column_rename_or_create(df02,'campa','skill','CMP_CL_OUT_ECO_A365_PORTA_IVR')
    else:
        df02=add_column_rename_or_create(df02,'campa','skill','NA')

    df02=add_column_rename_or_create(df02,'sugerido_1','sugerido_1','NA')
    df02 = df02[['recepcion','contact_info','sugerido_1','nombre_cliente','rut','id_plan','comuna','compania','skill','decil','percentil','id','nombre_archivo']]

    df02['nombre_cliente'] = df02['nombre_cliente'].fillna('NA')
    df02['id_plan'] = df02['id_plan'].fillna('NA')
    df02['id'] = df02['id'].fillna('NA')
    df02['rut'] = df02['rut'].fillna('NA')
    df02['comuna'] = df02['comuna'].fillna('NA')
    df02['compania'] = df02['compania'].fillna('NA')
    
    df02['contact_info'] = df02['contact_info'].apply(ultimos_8_caracteres)
    df02['comuna'] = df02['comuna'].apply(limpiar_texto)
    df02['comuna'] = df02['comuna'].apply(reemplazar_text)

    condicion_01 = [
    df02['nombre_archivo'].str.contains('delta', case=False),
    df02['nombre_archivo'].str.contains('juntos', case=False)
    ]

    # Valores a asignar
    choices = ['DELTA', 'JUNTOS']

    # Crear la nueva columna 'nuevo_columna'
    df02['adicional_01'] = np.select(condicion_01, choices, default='NA')

    return df02

# ------------------------------------+-+-+--------------------------------

# def cargar_archivo_csv(spark,filename):
#     filePath = os.path.join(ruta_base, filename)
#     return spark.read.csv(filePath, sep=';',header=True)

def preparar_tabla(df,fecha_recepcion):
    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    window_spec = Window.partitionBy("contact_info").orderBy(col("skill").asc(),col("adicional_01").desc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("comuna").desc())
    return df_01.withColumn("indice", row_number().over(window_spec))

def bloque_bases(spark,nombreDB,fecha_recepcion,dias_descanso):
    query1 = f"""
    SELECT
        distinct
        b.contact_info as contact_info_1 
        ,a.nombre_base_cargada  as nombre_base_cargada_1 
        ,a.fecha_carga
        ,a.nombre_skill AS nombre_skill_1 
    from DB_BaseSemanal.dbo.TB_BaseCargada a
    inner join TB_RegistroCargado b
    on a.id_baseCargada=b.id_baseCargada
    where a.fecha_carga between  DATEADD(day, -{dias_descanso}, '{fecha_recepcion}') and '{fecha_recepcion}'
    and b.id_detalle<>5
    """

    return obtener_tabla_sql(spark,nombreDB,query1)

def reducir_columnas(spark,df):
    df_02 = df.withColumn("descanso", 
                    (col("fecha_recepcion").cast("timestamp").cast("long") - col("fecha_carga").cast("timestamp").cast("long")) / (24 * 3600))
    df_02 = df_02.withColumn("descanso", when(col("descanso").isNull(), 0).otherwise(col("descanso")))

    df_02= df_02.withColumn("obs1",when(col("obs1") == 'duplicado', 'duplicado')
                            .when(col("nombre_base_cargada_1").isNull(), 'unico')
                            .when(col("descanso")<=45, 'sin descanso')
                            .otherwise("sin descanso mayor a 45 dias"))

    # df_02= df_02.withColumn("obs2",when(col("obs1") == 'duplicado', 'duplicado')
    #                         .when(col("obs1") == 'unico', 'unico')
    #                         .otherwise("sin descanso mayor a 45 dias"))

    df_02=add_comuna(spark,df_02)

    window_spec = Window.partitionBy("indice").orderBy(col("comuna").asc())
    df_02 = df_02.withColumn("hh_1", row_number().over(window_spec))
    df_02 = df_02.filter(col("hh_1") == 1).drop('hh_1')

    df_02=df_02.select('recepcion', 'contact_info', 'sugerido_1', 'nombre_cliente', 'rut', 'id_plan', 'comuna', 'region', 'compania', 'skill', 'decil', 'percentil', 'id', 'nombre_archivo','adicional_01','fecha_recepcion', 'nombre_skill', 'obs1', 'nombre_base_cargada_1', 'fecha_carga', 'descanso')

    return df_02

def add_comuna(spark,df):
    query1 = f"""
    select 
    comuna_ref as comuna
    ,region
    from TB_Comuna
    """
    df_comuna = obtener_tabla_sql(spark,'DB_BaseSemanal',query1)
    return df.join(df_comuna, ["comuna"], "left") 

# ------------------------------------+-+-+--------------------------------

def add_migraciones(spark,nameDB,fecha_recepcion,df): 
    df_bloque=bloque_bases(spark,nameDB,fecha_recepcion,45)

    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    
    df= df.withColumn("nombre_skill",when(col("skill") == 'CMP_CL_OUT_ECO_A365_MIGRACIONES', 'MIGRACIONES')
                    .when(col("skill") == 'CMP_CL_OUT_ECO_A365_MIGRACIONES2', 'MIGRACIONES2')
                    .when(col("skill") == 'CMP_CL_OUT_ECO_A365_MIGRACIONES3', 'CONECTA_MAYOR'))

    window_spec = Window.partitionBy("contact_info").orderBy(col("contact_info").asc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("nombre_skill").desc())
    df_01= df_01.withColumn("indice", row_number().over(window_spec))

    
    df_01= df_01.join(
        df_bloque, 
        (df_01["contact_info"] == df_bloque["contact_info_1"]) & 
        (df_01["obs1"] == 'unico') & 
        (df_01["fecha_recepcion"] > df_bloque["fecha_carga"]), 
        "left"
    )

    df_01= df_01.withColumn("ref1",when((col("nombre_skill_1").isNull()), 3)
                      .when((col("nombre_skill") == col("nombre_skill_1")) & (col("nombre_skill_1").isNotNull()), 1)
                     .otherwise(2))
    
    df_01= df_01.withColumn("ref2",when((col("fecha_carga").isNull()), 1).otherwise(2))
    
    window_spec = Window.partitionBy("indice").orderBy(col("ref2").desc(),col("fecha_carga").desc(),col("ref1").asc())
    df_01 = df_01.withColumn("rank", row_number().over(window_spec))
    return df_01.filter(col("rank") == 1).drop('ref2','ref1')

def add_perfilada(spark,nameDB,fecha_recepcion,df):
    df_bloque=bloque_bases(spark,nameDB,fecha_recepcion,45)

    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    
    df= df.withColumn("nombre_skill",when(col("skill") == 'CMP_CL_OUT_ECO_A365_PORTA_PERFILADA', 'PORTA_PERFILADA')
                     .when(col("skill") == 'CMP_CL_OUT_ECO_A365_PORTA_EXCLUSIVA', 'PORTA_PERFILADA2'))

    window_spec = Window.partitionBy("skill","contact_info").orderBy(col("contact_info").asc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("comuna").desc())
    df_01= df_01.withColumn("indice", row_number().over(window_spec))

    
    df_01= df_01.join(
        df_bloque, 
        (df_01["contact_info"] == df_bloque["contact_info_1"]) & 
        (df_01["fecha_recepcion"] > df_bloque["fecha_carga"]) & 
        (df_01["obs1"] == 'unico') & 
        (df_01["nombre_skill"] == df_bloque["nombre_skill_1"]), 
        "left"
    )

    
    window_spec = Window.partitionBy("indice").orderBy(col("fecha_carga").desc())
    df_01 = df_01.withColumn("rank", row_number().over(window_spec))
    return df_01.filter(col("rank") == 1)

def add_porta_ivr(spark,nameDB,fecha_recepcion,df):
    df_bloque=bloque_bases(spark,nameDB,fecha_recepcion,120)

    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    
    df= df.withColumn("nombre_skill",when(col("skill") == 'CMP_CL_OUT_ECO_A365_PORTA_IVR', 'PORTA_IVR'))

    window_spec = Window.partitionBy("skill","contact_info").orderBy(col("contact_info").asc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("comuna").desc())
    df_01= df_01.withColumn("indice", row_number().over(window_spec))

    
    df_01= df_01.join(
        df_bloque, 
        (df_01["contact_info"] == df_bloque["contact_info_1"]) & 
        (df_01["fecha_recepcion"] > df_bloque["fecha_carga"]) & 
        (df_01["obs1"] == 'unico') & 
        (df_01["nombre_skill"] == df_bloque["nombre_skill_1"]), 
        "left"
    )

    
    window_spec = Window.partitionBy("indice").orderBy(col("fecha_carga").desc())
    df_01 = df_01.withColumn("rank", row_number().over(window_spec))
    return df_01.filter(col("rank") == 1)

def add_recuperados(spark,nameDB,fecha_recepcion,df):
    df_bloque=bloque_bases(spark,nameDB,fecha_recepcion,45)

    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    
    df= df.withColumn("nombre_skill",when(col("skill") == 'CMP_CL_OUT_ECO_A365_PORTA_RECUPERADOS', 'PORTA_RECUPERADOS'))

    window_spec = Window.partitionBy("skill","contact_info").orderBy(col("contact_info").asc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("comuna").desc())
    df_01= df_01.withColumn("indice", row_number().over(window_spec))

    
    df_01= df_01.join(
        df_bloque, 
        (df_01["contact_info"] == df_bloque["contact_info_1"]) & 
        (df_01["obs1"] == 'unico') & 
        (df_01["fecha_recepcion"] > df_bloque["fecha_carga"]) & 
        (df_01["nombre_skill"] == df_bloque["nombre_skill_1"]), 
        "left"
    )

    window_spec = Window.partitionBy("indice").orderBy(col("fecha_carga").desc())
    df_01 = df_01.withColumn("rank", row_number().over(window_spec))
    return df_01.filter(col("rank") == 1)

def add_segundas(spark,nameDB,fecha_recepcion,df):
    df_bloque=bloque_bases(spark,nameDB,fecha_recepcion,45)

    df=df.withColumn('fecha_recepcion',lit(fecha_recepcion))
    
    df= df.withColumn("nombre_skill",when(col("skill") == 'CMP_CL_OUT_ECO_A365_SEGUNDAS_LINEAS', 'SEGUNDAS_LINEAS')
                     .when(col("skill") == 'CMP_CL_OUT_ECO_A365_SEGUNDAS_LINEAS2', 'SEGUNDAS_LINEAS2'))

    window_spec = Window.partitionBy("skill","contact_info").orderBy(col("contact_info").asc())
    df_01 = df.withColumn("valor_unico", row_number().over(window_spec))
    df_01= df_01.withColumn("obs1",when(col("valor_unico") == 1, 'unico').otherwise("duplicado")).drop('valor_unico')

    window_spec = Window.orderBy(col("comuna").desc())
    df_01= df_01.withColumn("indice", row_number().over(window_spec))

    
    df_01= df_01.join(
        df_bloque, 
        (df_01["contact_info"] == df_bloque["contact_info_1"]) & 
        (df_01["fecha_recepcion"] > df_bloque["fecha_carga"]) & 
        (df_01["obs1"] == 'unico') , 
        "left"
    )

    window_spec = Window.partitionBy("indice").orderBy(col("fecha_carga").desc())
    df_01 = df_01.withColumn("rank", row_number().over(window_spec))
    return df_01.filter(col("rank") == 1).drop('rank')


# ------------------------------------+-+-+--------------------------------

def formato_recepcion_base():
    filename_mappings = {
        'base_porta_exclusiva_especial_a365': ('DB_PERFILADA','porta_perfilada'),
        'mis_a365': ('DB_MIGRACIONES','migraciones'),
        'mis_conecta_mayor': ('DB_MIGRACIONES','migraciones'),
        'porta_ivr_pp_': ('DB_IVR', 'porta_ivr'),
        'base_porta_recuperados_a365': ('DB_RECUPERADOS','porta_recuperados'),
        'out_la_': ('DB_SEGUNDAS','segundas_lineas'),
    }


    file_list=[file for file in os.listdir(ruta_base)
        if "~$" not in file 
            and "copia" not in file.lower() 
            and ".xlsx" in file.lower()]

    df_recepcion_base_out = []
    if file_list:
        for filename in file_list:
            filename_lower = filename.lower()
            valor_map = filename_mappings.get(next((key for key in filename_mappings if key in filename_lower), None))
            if valor_map is not None:
                # print(filename_lower)
                nameDB,nombre_campana = valor_map
                
                file_xlsx = os.path.join(ruta_base, filename)
                df01 = pd.read_excel(file_xlsx)
                df01['nombre_archivo'] = filename
                
                df02 = construir_tabla(df01,nombre_campana)
                df_recepcion_base_out.append(df02)
                
    if df_recepcion_base_out:
        df_final = pd.concat(df_recepcion_base_out, ignore_index=True)

    valores_recepcion = df_final['recepcion'].unique()
    for recepcion in valores_recepcion:
        df_temporal = df_final[df_final['recepcion'] == recepcion]
        
        temporal = f'recepcion_{recepcion}.csv'
        df_temporal.to_csv(f'{ruta_base}\\{temporal}', index=False, sep=';')

# ------------------------------------+-+-+--------------------------------
# ------------------------------------+-+-+--------------------------------










# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------

def cargar_excel(filename):
    file_xlsx = os.path.join(ruta_base, filename)
    df01 = pd.read_excel(file_xlsx)
    df01['nombre_archivo'] = filename
    return df01

def archivo_csv_disponible(df,nombre_archivo):

    temporal = f'{nombre_archivo}.csv'
    df.to_csv(f'{ruta_base}\\{temporal}', index=False, sep=';')

def eliminar_archivo(nombre_archivo):
    ruta_archivo = os.path.join(ruta_base, nombre_archivo)
    if os.path.exists(ruta_archivo):
        os.remove(ruta_archivo)
        print(f'Archivo {nombre_archivo} eliminado.')
    else:
        print(f'El archivo {nombre_archivo} no existe.')



# Cargar registros para TB_agentSumaryHour - Subhour
#






# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------
































# -----------------------------------------------------------------------------------------------------------------------------------------------

def semana_carga_formato_base_spark():
    
    spark = SparkSession.builder \
        .appName("SparkExample") \
        .master("local[*]") \
        .config('spark.driver.extraClassPath', 'C:/spark/jars/mssql-jdbc-10.2.3.jre17.jar') \
        .config('spark.executor.extraClassPath', 'C:/spark/jars/mssql-jdbc-10.2.3.jre17.jar') \
        .config('spark.executor.memory', '4g') \
        .config('spark.driver.memory', '4g') \
        .getOrCreate()


    filename_mappings = {
        'migras3': ('DB_MIGRACIONES', headers_dic['migras3'],construir_tabla_migras),
        'migras': ('DB_MIGRACIONES',headers_dic['migras_v2'],construir_tabla_migras),
        'segundas': ('DB_SEGUNDAS', headers_dic['segundas'],construir_tabla_segundas),
        'perfilada': ('DB_PERFILADA', headers_dic['porta'],construir_tabla_porta),
        'piloto_ivr': ('DB_IVR', headers_dic['porta'],construir_tabla_porta),
        'recuperados': ('DB_RECUPERADOS', headers_dic['recuperados'],construir_tabla_recuperados),
        'ivr': ('DB_IVR', headers_dic['ivr'],construir_tabla_ivr),
        'empresas_fijo': ('DB_EMPRESAS', headers_dic['emp_fijo'],construir_tabla_fibra_hogar),
        'empresas_laser': ('DB_EMPRESAS', headers_dic['emp_laser'],construir_tabla_fibra_hogar_laser),
        'empresas_cross': ('DB_EMPRESAS', headers_dic['emp_cross'],construir_tabla_fibra_hogar_cross),
        'equipo': ('DB_HOGAR', headers_dic['emp_fibra_cross'],construir_tabla_fibra_cross),
    }

    file_list=[file for file in os.listdir(ruta_base)
        if "~$" not in file and "copia" not in file.lower() and "fc_" in file.lower()]

    if file_list:
        for filename in file_list:
            filename_lower = filename.lower()
            valor_map = filename_mappings.get(next((key for key in filename_mappings if key in filename_lower), None))

            if valor_map is None:
                print(f'No se encontró formato para {filename}')
            else:
                nameDB, header, preparar_base_campana = valor_map
                df = obtener_archivo_base_campana(spark, filename, header)

                df_list_semana=construir_tabla_bases(spark,df,filename)
                if not df_list_semana.isEmpty():
                    Export_list_base_sql(df_list_semana,'DB_BaseSemanal','TB_BaseCargada')
                    
                    df_registro_cargado = preparar_base_campana(df)
                    df_registro_cargado1=add_columnas(spark,filename,df_registro_cargado)
                    Export_list_base_sql(df_registro_cargado1,nameDB,'TB_RegistroCargado')
                    # print(f'registros del archivo {filename} cargados')
                    mover_archivo(ruta_base,filename)
                
                else:
                    print('Sin archivos para procesar')

    spark.stop()

# -----------------------------------------------------------------------------------------------------------------------------------------------

def dia_carga_formato_rsl_spark():
    spark = SparkSession.builder \
        .appName("SparkExample") \
        .master("local[*]") \
        .config('spark.driver.extraClassPath', 'C:/spark/jars/mssql-jdbc-10.2.3.jre17.jar') \
        .config('spark.executor.extraClassPath', 'C:/spark/jars/mssql-jdbc-10.2.3.jre17.jar') \
        .config('spark.executor.memory', '4g') \
        .config('spark.driver.memory', '4g') \
        .getOrCreate()

    mappings_01_carga_rsl = {
        'migras3': ('DB_MIGRACIONES', headers_dic['migras3']),
        'migras': ('DB_MIGRACIONES', headers_dic['migras_v2']),
        'segundas': ('DB_SEGUNDAS', headers_dic['segundas']),
        'perfilada': ('DB_PERFILADA', headers_dic['porta']),
        'recuperados': ('DB_RECUPERADOS', headers_dic['recuperados']),
        'piloto_ivr': ('DB_IVR', headers_dic['porta']),
        'ivr': ('DB_IVR', headers_dic['ivr']),
        'empresas_fijo': ('DB_EMPRESAS', headers_dic['emp_fijo']),
        'empresas_laser': ('DB_EMPRESAS', headers_dic['emp_laser']),
        'empresas_cross': ('DB_EMPRESAS', headers_dic['emp_cross']),
        'equipo': ('DB_HOGAR', headers_dic['emp_fibra_cross']),
    }


    file_list=[file for file in os.listdir(ruta_base)
        if "~$" not in file and "copia" not in file.lower() and "discado_" in file.lower() and ".rsl" in file.lower()]

    if file_list:
        for filename in file_list:
            filename_lower = filename.lower()
            valor_map = mappings_01_carga_rsl.get(next((key for key in mappings_01_carga_rsl if key in filename_lower), None))

            if valor_map is None:
                print(f'No se encontró formato para {filename}')
            else:
                nameDB, header = valor_map
                df = obtener_archivo_base_campana(spark, filename, header)

                AgregarTipisNuevas(spark,df,nameDB,filename)
                df_rsl_dia=construir_tabla_discado(spark,df,nameDB,filename)

                if not df_rsl_dia.isEmpty():
                    Export_list_base_sql(df_rsl_dia,"DB_RSL","TB_discado_1")
                    # print(f"{filename} subido")
                    mover_archivo(ruta_base,filename)

    else:
        print('Sin archivos para procesar')

    spark.stop()
