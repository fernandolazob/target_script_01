import sys
import os

ruta_actual = os.path.dirname(__file__)
ruta_funciones = os.path.abspath(os.path.join(ruta_actual, "..", "funciones"))

sys.path.append(ruta_funciones)

from funciones import *
from funciones_spark import *
from variables_inicio import *
from utils_sql import *

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc

# =====================================================
# CONFIGURACION
# =====================================================
st.set_page_config(layout="wide")

st.title("📊 Generador de Lista Diners TC")

# =====================================================
# CARGAR DATA DESDE SQL SERVER
# =====================================================
query = """
SELECT *
FROM CRONOX.dbo.borrar_tc_dinner
"""

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server_sa};"
    f"DATABASE={db_sa};"
    f"UID={user_sa};"
    f"PWD={pwd_sa};"
)

df = pd.read_sql(query, conn)
conn.close()

# =====================================================
# CREAR COLUMNA BENCH EN PANDAS
# =====================================================
df["bench"] = df["title"].where(
    df["title"].isin(["l_ibk", "l_bbva", "l_sco", "l_bcp"]),
    "Otros"
)

# =====================================================
# CONVERTIR FECHAS
# =====================================================
if "fecha_llamada" in df.columns:
    df["fecha_llamada"] = pd.to_datetime(df["fecha_llamada"], errors="coerce")

# =====================================================
# FUNCIONES AUXILIARES
# =====================================================
def opciones(columna):
    if columna in df.columns:
        return sorted(df[columna].dropna().astype(str).unique())
    return []


def aplicar_filtro(df_base, columna, valores):
    if valores and columna in df_base.columns:
        return df_base[df_base[columna].astype(str).isin(valores)]
    return df_base


# =====================================================
# SEGMENTADORES
# =====================================================
st.sidebar.title("Segmentadores")

seg_edad = st.sidebar.multiselect(
    "Seg Edad",
    opciones("seg_edad")
)

seg_oferta = st.sidebar.multiselect(
    "Seg Oferta",
    opciones("seg_oferta")
)

bench = st.sidebar.multiselect(
    "Bench",
    opciones("bench")
)

city = st.sidebar.multiselect(
    "City",
    opciones("city")
)

segmentacion = st.sidebar.multiselect(
    "Segmentación",
    opciones("segmentacion")
)

seg_monto = st.sidebar.multiselect(
    "Segmento Monto",
    opciones("seg_monto")
)

seg_tea = st.sidebar.multiselect(
    "Segmento TEA",
    opciones("seg_tea")
)

# Opcional: filtro por fecha llamada
fecha_llamada = None

if "fecha_llamada" in df.columns:
    fecha_llamada = st.sidebar.date_input(
        "Fecha llamada",
        value=None
    )

# =====================================================
# FILTRAR DATA
# =====================================================
df_filtrado = df.copy()

df_filtrado = aplicar_filtro(df_filtrado, "seg_edad", seg_edad)
df_filtrado = aplicar_filtro(df_filtrado, "seg_oferta", seg_oferta)
df_filtrado = aplicar_filtro(df_filtrado, "bench", bench)
df_filtrado = aplicar_filtro(df_filtrado, "city", city)
df_filtrado = aplicar_filtro(df_filtrado, "segmentacion", segmentacion)
df_filtrado = aplicar_filtro(df_filtrado, "seg_monto", seg_monto)
df_filtrado = aplicar_filtro(df_filtrado, "seg_tea", seg_tea)

if fecha_llamada and "fecha_llamada" in df_filtrado.columns:
    df_filtrado = df_filtrado[
        df_filtrado["fecha_llamada"].dt.date == fecha_llamada
    ]

# =====================================================
# KPI
# =====================================================
col1, col2, col3 = st.columns(3)

col1.metric("Registros filtrados", len(df_filtrado))

if "vendor_lead_code" in df_filtrado.columns:
    col2.metric("Clientes únicos", df_filtrado["vendor_lead_code"].nunique())
else:
    col2.metric("Clientes únicos", 0)

if "phone_number" in df_filtrado.columns:
    col3.metric("Teléfonos únicos", df_filtrado["phone_number"].nunique())
else:
    col3.metric("Teléfonos únicos", 0)

st.divider()

# =====================================================
# GRAFICO 1: BENCH
# =====================================================
st.subheader("Distribución por Bench")

if not df_filtrado.empty:
    conteo_bench = df_filtrado["bench"].value_counts()

    fig, ax = plt.subplots(figsize=(8, 5))
    conteo_bench.plot(kind="barh", ax=ax)

    ax.set_xlabel("Cantidad")
    ax.set_ylabel("Bench")
    ax.set_title("Cantidad por Bench")

    for i, v in enumerate(conteo_bench.values):
        ax.text(v, i, str(v), va="center")

    st.pyplot(fig)
else:
    st.warning("No hay datos para mostrar.")

# =====================================================
# GRAFICO 2: SEGMENTACION
# =====================================================
if "segmentacion" in df_filtrado.columns:
    st.subheader("Distribución por Segmentación")

    if not df_filtrado.empty:
        conteo_segmentacion = df_filtrado["segmentacion"].value_counts()

        fig2, ax2 = plt.subplots(figsize=(8, 5))
        conteo_segmentacion.plot(kind="barh", ax=ax2)

        ax2.set_xlabel("Cantidad")
        ax2.set_ylabel("Segmentación")
        ax2.set_title("Cantidad por Segmentación")

        for i, v in enumerate(conteo_segmentacion.values):
            ax2.text(v, i, str(v), va="center")

        st.pyplot(fig2)

# =====================================================
# COLUMNAS FINALES PARA DESCARGA
# =====================================================
columnas_finales = [
    "vendor_lead_code",
    "phone_number",
    "first_name",
    "last_name",
    "title",
    "address1",
    "address2",
    "address3",
    "city",
    "province",
    "email",
    "security_phrase",
    "comments"
]

columnas_existentes = [c for c in columnas_finales if c in df_filtrado.columns]

df_final = df_filtrado[columnas_existentes].copy()

# =====================================================
# TABLA FINAL
# =====================================================
st.subheader("Base filtrada")

st.dataframe(
    df_final,
    use_container_width=True
)

st.write("Total registros:", df_final.shape[0])

# =====================================================
# DESCARGA CSV
# =====================================================
csv = df_final.to_csv(index=False, sep=";").encode("utf-8-sig")

st.download_button(
    label="⬇ Descargar base",
    data=csv,
    file_name="base_diners_tc.csv",
    mime="text/csv"
)