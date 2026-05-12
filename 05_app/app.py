import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# =========================
# CONFIG STREAMLIT
# =========================

st.set_page_config(page_title="Filtro de base", layout="wide")
st.title("TC dinners")

# Para ejecutar:
# streamlit run app.py


# =========================
# CONEXIÓN SQL SERVER
# =========================

user_sa = "sa"
pwd_sa = "target2023$"
server_sa = "192.168.2.50"
db_sa = "CRONOX"

engine = create_engine(
    f"mssql+pyodbc://{user_sa}:{pwd_sa}@{server_sa}/{db_sa}?driver=ODBC+Driver+17+for+SQL+Server"
)

query = """
SELECT *
FROM cronox.dbo.borrar_tc_dinner
"""


# =========================
# CARGA DE DATA CON CACHE
# =========================

@st.cache_data
def cargar_data():
    df = pd.read_sql(query, engine)

    if "vendor_lead_code" in df.columns:
        df["vendor_lead_code"] = (
            df["vendor_lead_code"]
            .astype(str)
            .str.zfill(8)
        )

    return df


# =========================
# BOTÓN RESET + RECARGA
# =========================

if st.button("🔄 Resetear filtros y recargar tabla"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()


df = cargar_data()
df_filtrado = df.copy()


# =========================
# FILTROS SIMPLES
# =========================

st.sidebar.header("Filtros simples")

filtros_simples = [
    'city',
    'fecha_envio',
    'retiro'
]

for col in filtros_simples:
    if col in df_filtrado.columns:
        opciones = sorted(df_filtrado[col].dropna().astype(str).unique())

        seleccion = st.sidebar.multiselect(
            f"Filtrar {col}",
            opciones,
            key=f"filtro_simple_{col}"
        )

        if seleccion:
            df_filtrado = df_filtrado[
                df_filtrado[col].astype(str).isin(seleccion)
            ]


# =========================
# FILTRO TIPO TELÉFONO
# =========================

st.sidebar.header("Filtro teléfono")

if 'tipo_telf' in df_filtrado.columns:
    opciones_tipo = sorted(df_filtrado['tipo_telf'].dropna().astype(str).unique())

    tipo_telf_sel = st.sidebar.multiselect(
        "Seleccionar tipo_telf",
        opciones_tipo,
        key="filtro_tipo_telf"
    )

    if tipo_telf_sel:
        df_filtrado = df_filtrado[
            df_filtrado['tipo_telf'].astype(str).isin(tipo_telf_sel)
        ]


# =========================
# FILTROS NUMÉRICOS
# =========================

st.sidebar.header("Filtros por rango")

if 'edad' in df_filtrado.columns:
    df_filtrado['edad'] = pd.to_numeric(df_filtrado['edad'], errors='coerce')

    edad_min = int(df_filtrado['edad'].min()) if df_filtrado['edad'].notna().any() else 0
    edad_max = int(df_filtrado['edad'].max()) if df_filtrado['edad'].notna().any() else 100

    rango_edad = st.sidebar.slider(
        "Rango edad",
        min_value=edad_min,
        max_value=edad_max,
        value=(edad_min, edad_max),
        key="filtro_edad"
    )

    df_filtrado = df_filtrado[
        df_filtrado['edad'].between(rango_edad[0], rango_edad[1])
    ]


if 'linea_credito' in df_filtrado.columns:
    df_filtrado['linea_credito'] = pd.to_numeric(
        df_filtrado['linea_credito'],
        errors='coerce'
    )

    linea_min = int(df_filtrado['linea_credito'].min()) if df_filtrado['linea_credito'].notna().any() else 0
    linea_max = int(df_filtrado['linea_credito'].max()) if df_filtrado['linea_credito'].notna().any() else 0

    rango_linea = st.sidebar.slider(
        "Rango línea crédito",
        min_value=linea_min,
        max_value=linea_max,
        value=(linea_min, linea_max),
        key="filtro_linea_credito"
    )

    df_filtrado = df_filtrado[
        df_filtrado['linea_credito'].between(rango_linea[0], rango_linea[1])
    ]


# =========================
# FILTROS TIPO MATRIZ
# =========================

st.subheader("Filtros tipo matriz")

filtros_matriz = [
    'n_base',
    'segmentacion',
    'provincia',
    'prob_contacto',
    'seg_edad',
    'seg_oferta',
    'fecha_llamada',
    'q_intentos',
    'q_intentos_telf',
    'q_intentos_dia',
    'dni_unico',
    'mejor_estado_tipi_cli',
    'mejor_descripcion_cli',
    'mejor_estado_tipi_telf',
    'mejor_descripcion_telf',
    'mejor_sub_descripcion_telf'
]

with st.expander("Abrir filtros avanzados", expanded=False):

    for col in filtros_matriz:
        if col in df_filtrado.columns:

            opciones = (
                df_filtrado[col]
                .dropna()
                .astype(str)
                .value_counts()
                .reset_index()
            )

            opciones.columns = [col, 'cantidad']

            st.markdown(f"### {col}")

            texto_busqueda = st.text_input(
                f"Buscar en {col}",
                key=f"buscar_{col}"
            )

            if texto_busqueda:
                opciones = opciones[
                    opciones[col].str.contains(
                        texto_busqueda,
                        case=False,
                        na=False
                    )
                ]

            valores = opciones[col].tolist()

            seleccion = st.multiselect(
                f"Seleccionar valores de {col}",
                valores,
                key=f"select_{col}"
            )

            st.dataframe(opciones, use_container_width=True)

            if seleccion:
                df_filtrado = df_filtrado[
                    df_filtrado[col].astype(str).isin(seleccion)
                ]


# =========================
# RESULTADO FINAL
# =========================

st.subheader("Resultado filtrado")

st.write(f"Registros originales: **{len(df):,}**")
st.write(f"Registros finales: **{len(df_filtrado):,}**")

st.dataframe(df_filtrado, use_container_width=True)


# =========================
# DESCARGA SOLO COLUMNAS VICIDIAL
# =========================

columnas_descarga = [
    'vendor_lead_code',
    'phone_number',
    'title',
    'first_name',
    'last_name',
    'address1',
    'address2',
    'address3',
    'city',
    'province',
    'email',
    'security_phrase',
    'comments'
]

columnas_existentes = [
    col for col in columnas_descarga
    if col in df_filtrado.columns
]

df_descarga = df_filtrado[columnas_existentes]

csv = df_descarga.to_csv(index=False).encode("utf-8-sig")

st.download_button(
    label="⬇️ Descargar base filtrada CSV",
    data=csv,
    file_name="base_filtrada.csv",
    mime="text/csv"
)