import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# ---------- CONFIGURACION ----------
st.set_page_config(layout="wide")

st.title("📊 Generador de Base Cencosud Prestamo")

# ---------- CARGAR DATA ----------
ruta = "C:/Users/DATA/Documents/datos/01_script/inicio/dataset.csv"

df = pd.read_csv(ruta, sep=";")

# convertir fecha a formato fecha
df["fecha_llamada"] = pd.to_datetime(df["fecha_llamada"], errors="coerce")

# ---------- SEGMENTADORES ----------
st.sidebar.title("Segmentadores")

tipo_telf = st.sidebar.multiselect(
    "Tipo Teléfono",
    df["tipo_telf"].dropna().unique()
)

regimen = st.sidebar.multiselect(
    "Regimen laboral",
    df["regimen_laboral"].dropna().unique()
)

marca = st.sidebar.multiselect(
    "Marca",
    df["marca"].dropna().unique()
)

tipo_base = st.sidebar.multiselect(
    "Tipo base",
    df["tipo_base"].dropna().unique()
)

propension = st.sidebar.multiselect(
    "Propensión",
    df["PROPENSION"].dropna().unique()
)

seg_monto = st.sidebar.multiselect(
    "Segmento monto",
    df["seg_monto"].dropna().unique()
)

seg_tea = st.sidebar.multiselect(
    "Segmento TEA",
    df["seg_tea"].dropna().unique()
)

city = st.sidebar.multiselect(
    "Ciudad",
    df["city"].dropna().unique()
)

tipo_estado = st.sidebar.multiselect(
    "Tipo Estado",
    df["tipo_estado"].dropna().unique()
)

descripcion_cli = st.sidebar.multiselect(
    "Descripción Cliente",
    df["mejor_descripcion_cli"].dropna().unique()
)

fecha_llamada = st.sidebar.date_input(
    "Fecha llamada"
)

# ---------- FILTRAR DATA ----------
df_filtrado = df.copy()

if propension:
    df_filtrado = df_filtrado[df_filtrado["PROPENSION"].isin(propension)]

if seg_monto:
    df_filtrado = df_filtrado[df_filtrado["seg_monto"].isin(seg_monto)]

if seg_tea:
    df_filtrado = df_filtrado[df_filtrado["seg_tea"].isin(seg_tea)]

if tipo_telf:
    df_filtrado = df_filtrado[df_filtrado["tipo_telf"].isin(tipo_telf)]

if regimen:
    df_filtrado = df_filtrado[df_filtrado["regimen_laboral"].isin(regimen)]

if marca:
    df_filtrado = df_filtrado[df_filtrado["marca"].isin(marca)]

if tipo_base:
    df_filtrado = df_filtrado[df_filtrado["tipo_base"].isin(tipo_base)]

if city:
    df_filtrado = df_filtrado[df_filtrado["city"].isin(city)]

if tipo_estado:
    df_filtrado = df_filtrado[df_filtrado["tipo_estado"].isin(tipo_estado)]

if descripcion_cli:
    df_filtrado = df_filtrado[df_filtrado["mejor_descripcion_cli"].isin(descripcion_cli)]

if fecha_llamada:
    df_filtrado = df_filtrado[
        df_filtrado["fecha_llamada"].dt.date == fecha_llamada
    ]

# ---------- KPI ----------
col1, col2, col3 = st.columns(3)

col1.metric("Registros filtrados", df_filtrado.shape[0])
col2.metric("Clientes únicos", df_filtrado["vendor_lead_code"].nunique())
col3.metric("Teléfonos únicos", df_filtrado["phone_number"].nunique())

st.divider()

# ---------- GRAFICO PIE ----------
st.subheader("Distribución Tipo Estado (Clientes únicos)")

if not df_filtrado.empty:

    df_unico = df_filtrado.drop_duplicates("vendor_lead_code")

    conteo_estado = df_unico["tipo_estado"].value_counts()

    fig1, ax1 = plt.subplots()

    ax1.pie(
        conteo_estado,
        labels=conteo_estado.index,
        autopct="%1.1f%%"
    )

    ax1.set_title("Tipo Estado")

    st.pyplot(fig1)

else:
    st.write("No hay datos para mostrar")

# ---------- GRAFICO RESULTADOS ----------
st.subheader("Top Resultados de Gestión")

if not df_filtrado.empty:

    conteo_resultado = (
        df_filtrado["mejor_descripcion_cli"]
        .value_counts()
        .head(10)
    )

    st.bar_chart(conteo_resultado)

# ---------- COLUMNAS FINALES ----------
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

df_final = df_filtrado[columnas_finales]

# ---------- TABLA ----------
st.subheader("Base filtrada")

st.dataframe(
    df_final,
    use_container_width=True
)

st.write("Total registros:", df_final.shape[0])

# ---------- DESCARGAR ----------
csv = df_final.to_csv(index=False, sep=";")

st.download_button(
    "⬇ Descargar base",
    csv,
    "base_cencosud.csv",
    "text/csv"
)