import streamlit as st
import pandas as pd

st.title("Dashboard de Gestión")

# cargar datos
df = pd.read_csv("dataset.csv")

st.write("Dataset")
st.dataframe(df)