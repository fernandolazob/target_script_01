from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.9+
import pandas as pd
import numpy as np
import re
import os
import unicodedata
# import mariadb
from math import ceil
import builtins
from sqlalchemy import create_engine
import urllib
from io import BytesIO


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
import mysql.connector
import mysql.connector
import pandas as pd

def update_mysql_en_bloques(df,tabla,periodo,col_llave_mysql,col_valor_mysql,col_llave_df,col_valor_df,host,user,password,database,port=3306,batch_size=10000,validar_sin_grabar=False):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    cursor = conn.cursor()

    sql = f"""
        UPDATE {tabla}
        SET {col_valor_mysql} = %s
        WHERE {col_llave_mysql} = %s
        AND cl_base = %s
    """

    df_tmp = df[[col_llave_df, col_valor_df]].copy()

    # limpiar llave tipo DNI/documento
    df_tmp[col_llave_df] = (
        df_tmp[col_llave_df]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(8)
    )

    # limpiar valor si viene como nan
    df_tmp[col_valor_df] = df_tmp[col_valor_df].where(
        pd.notnull(df_tmp[col_valor_df]),
        None
    )

    # quitar registros sin llave
    df_tmp = df_tmp.dropna(subset=[col_llave_df])
    df_tmp = df_tmp[df_tmp[col_llave_df] != ""]

    # orden: valor nuevo, llave, periodo
    data = [
        (valor, llave, periodo)
        for valor, llave in df_tmp[[col_valor_df, col_llave_df]]
        .itertuples(index=False, name=None)
    ]

    total = len(data)
    print(f"Total registros a procesar: {total}")

    try:
        total_afectadas = 0

        for i in range(0, total, batch_size):
            batch = data[i:i + batch_size]

            cursor.executemany(sql, batch)

            filas_lote = cursor.rowcount
            total_afectadas += filas_lote

            if validar_sin_grabar:
                conn.rollback()
                print(f"Lote {i} - {i + len(batch)} probado SIN grabar | filas afectadas: {filas_lote}")
                break
            else:
                conn.commit()
                print(f"Lote {i} - {i + len(batch)} actualizado | filas afectadas: {filas_lote}")

        print(f"Proceso terminado. Total filas afectadas: {total_afectadas}")

    except Exception as e:
        conn.rollback()
        print("Error actualizando MySQL:", e)

    finally:
        cursor.close()
        conn.close()

def exec_query_sql(server, db, user, pwd, query, descripcion):

    import pyodbc
    import time

    conn = None
    cursor = None
    inicio = time.time()

    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={pwd};"
            "TrustServerCertificate=yes;"
        )

        cursor = conn.cursor()

        cursor.execute(f"{query}")
        conn.commit()

        duracion = round(time.time() - inicio, 2)
        print(f"{descripcion} | realizado | duración: {duracion} seg")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"{descripcion} | ERROR:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
def ejecutar_sql_server(server, db, user_01, pwd, sql):
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"UID={user_01};"
        f"PWD={pwd};"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

# import datetime as dt
# import paramiko

# import datetime
# import json
# import time
# import subprocess    

# from selenium import webdriver
# import time, os, glob, shutil, datetime, random

# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait, Select
# from selenium.webdriver.support import expected_conditions as EC

# py -3.13 -m pip install sqlalchemy pyodbc

# import sys

def fecha_a_nombre(fecha_mes_base):
    from datetime import datetime

    fecha = datetime.strptime(fecha_mes_base, "%Y-%m-%d")

    meses = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]

    return f"{meses[fecha.month - 1]} {fecha.year}"

def pareto_mejor_descripcion(df,tipificaicon_telf):

    conteo = df[f'{tipificaicon_telf}'] \
                .value_counts() \
                .sort_values(ascending=False)

    porc_acum = conteo.cumsum() / conteo.sum() * 100

    fig, ax1 = plt.subplots(figsize=(12,6))

    bars = ax1.bar(conteo.index, conteo.values, color="steelblue")
    ax1.set_ylabel("Cantidad")

    for bar in bars:
        height = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width()/2,
            height,
            f'{int(height)}',
            ha='center',
            va='bottom'
        )

    plt.xticks(rotation=45, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(conteo.index, porc_acum, color="red", marker="o")
    ax2.set_ylabel("% Acumulado")

    for i, txt in enumerate(porc_acum):
        ax2.text(i, txt, f"{txt:.1f}%", color="red", ha="center", va="bottom")

    ax2.axhline(80, color="green", linestyle="--")

    plt.title("Pareto - Mejor_Descripcion_telef")
    plt.tight_layout()
    plt.show()




# def transact_sql_delete_mes_actual_consolidado(fecha_fin):
#     try:
#         conexion = pyodbc.connect(conn_str)
#         cursor = conexion.cursor()
#         query = f"""
#             delete from DB_A365.dbo.edu03_asistencia
#             where fecha_ref_reporte =DATEADD(MONTH, DATEDIFF(MONTH, 0, '{fecha_fin}'), 0)
#         """
#         cursor.execute(query)
#         conexion.commit()
        
#         conexion.close()
#     except Exception as e:
#         (f"Error durante la inserción: {e}")
#     finally:
#         engine.dispose()






