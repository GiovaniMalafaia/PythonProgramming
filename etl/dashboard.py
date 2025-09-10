# dashboard.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()
user = os.getenv("user")
password = os.getenv("password")
db = os.getenv("db")

# Conexão PostgreSQL (Neon)
conn_str = f"postgresql+psycopg2://{user}:{password}@ep-still-frog-ad9qpe84-pooler.c-2.us-east-1.aws.neon.tech/{db}?sslmode=require"
engine = create_engine(conn_str)

# Título do dashboard
st.title("📊 Dashboard de Cotações de Moedas")

# Carrega dados do banco
query = "SELECT * FROM public.taxa_cambio;"
df = pd.read_sql(query, engine)

# Exibe dados brutos
st.subheader("📋 Dados Brutos")
st.dataframe(df)

# Filtro de moeda
moedas = df["moeda"].unique().tolist()
moeda_selecionada = st.selectbox("Selecione a moeda", moedas)

df_filtrada = df[df["moeda"] == moeda_selecionada]

# Exibe informações
st.subheader(f"💱 Cotação do {moeda_selecionada}")
st.metric(label=f"Valor atual ({moeda_selecionada})", value=df_filtrada["taxa"].values[0])

# Gráfico simples
st.subheader("📈 Evolução das Cotações")
st.line_chart(df.set_index("moeda")["taxa"])