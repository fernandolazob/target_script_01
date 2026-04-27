# app.py
import streamlit as st
from pyspark.sql import SparkSession, functions as F
import pandas as pd
from io import BytesIO

from funciones import *
from funciones_spark import *
from variables_inicio import *
from utils_sql import *

st.set_page_config(page_title="Generador de listas Vicidial", layout="wide")

st.title("Generador dinámico de listas para Vicidial")

# =========================
# SPARK
# =========================
@st.cache_resource
def crear_spark():
    return SparkSession.builder \
        .appName("AppListasVicidial") \
        .master("local[*]") \
        .config('spark.driver.extraClassPath', 'C:/spark/jars/mssql-jdbc-13.2.1.jre11.jar') \
        .config('spark.executor.extraClassPath', 'C:/spark/jars/mssql-jdbc-13.2.1.jre11.jar') \
        .config('spark.executor.memory', '8g') \
        .config('spark.driver.memory', '8g') \
        .getOrCreate()

spark = crear_spark()

# =========================
# CARGA
# =========================
query = """
SELECT *
FROM cronox.dbo.borrar_alfin
"""

@st.cache_data
def cargar_data():
    df = obtener_tabla_sql(spark, query, server_sa, user_sa, pwd_sa, db_sa)
    return df

df_list = cargar_data()

st.success("Base cargada correctamente")

# =========================
# FILTROS WEB
# =========================
st.sidebar.header("Filtros")

fecha_corte = st.sidebar.date_input("Fecha máxima llamada")
propension = st.sidebar.multiselect(
    "Propensión IC",
    ["1", "2", "3", "4", "5"],
    default=["1"]
)

frescura = st.sidebar.multiselect(
    "Frescura",
    ["0", "1", "2", "3", "4"],
    default=["0", "1"]
)

# =========================
# FILTRADO
# =========================
df_filtrado = df_list.filter(
    ((F.col('mejor_descripcion_cli').isin(mejor15_descripcion_telf)) | (F.col('mejor_descripcion_cli').isNull())) &
    ((F.col('mejor15_descripcion_telf').isin(mejor15_descripcion_telf)) | (F.col('mejor15_descripcion_telf').isNull())) &
    (F.col('fecha_llamada') < F.lit(str(fecha_corte))) &
    (F.col('retiro').isin(retiro)) &
    (F.col('propension_ic').isin(propension)) &
    (F.col('frescura').isin(frescura))
)

total = df_filtrado.count()

st.metric("Registros filtrados", total)

# =========================
# VISTA PREVIA
# =========================
df_pandas = df_filtrado.limit(5000).toPandas()

st.subheader("Vista previa")
st.dataframe(df_pandas, use_container_width=True)

# =========================
# EXPORTAR EXCEL
# =========================
def convertir_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="lista")
    return output.getvalue()

excel = convertir_excel(df_filtrado.toPandas())

st.download_button(
    label="Descargar Excel para Vicidial",
    data=excel,
    file_name="lista_vicidial.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)