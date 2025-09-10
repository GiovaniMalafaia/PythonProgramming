from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import logging

# Diretório base
caminho = os.path.dirname(os.path.abspath(__file__))
pasta_gold = os.path.join(caminho, "gold")

data_atual = datetime.today().strftime("%Y-%m-%d")

# Carrega .env
load_dotenv()

user = os.getenv("user")
password = os.getenv("password")
db = os.getenv("db")

# Configuracao do logging
log_file = os.path.join(pasta_gold, f"logs\\psql_{data_atual}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def conn_psql():
    if not all([user, password, db]):
        print(user, password, db)
        logging.info("Erro: Variáveis de ambiente não foram carregadas. Verifique o arquivo .env")

    try:
        # Conexão com Neon
        conn_str = f"postgresql+psycopg2://{user}:{password}@ep-still-frog-ad9qpe84-pooler.c-2.us-east-1.aws.neon.tech/{db}?sslmode=require"
        engine = create_engine(conn_str)
        logging.info(f"Conexao realizada com sucesso no banco {db}.")
        return engine
    
    except Exception as e:
        logging.error(f"Erro ao se conectar no banco {db}:", e)
        return None

def insert_psql():
    # Pega o último arquivo Parquet da pasta gold
    arquivos_gold = sorted([f for f in os.listdir(pasta_gold) if f.endswith(".parquet")])
    if not arquivos_gold:
        logging.info("Nenhum arquivo Parquet encontrado em /gold/")

    ultimo_arquivo = arquivos_gold[-1]
    caminho_parquet = os.path.join(pasta_gold, ultimo_arquivo)
    print(f"Lendo arquivo Parquet: {caminho_parquet}")
    logging.info(f"Lendo arquivo Parquet: {caminho_parquet}")

    # Carrega Parquet
    df = pd.read_parquet(caminho_parquet, engine="pyarrow")

    df["data_atualizacao"] = datetime.now()

    conexao = conn_psql()

    # Inserção no banco
    try:
        with conexao.begin() as conn:
            # Insere os dados do DataFrame
            df.to_sql("taxa_cambio", conn, schema="public", if_exists="append", index=False)
            print(f"{len(df)} registros inseridos com sucesso na tabela public.taxa_cambio!")
            logging.info(f"{len(df)} registros inseridos com sucesso na tabela public.taxa_cambio!")

    except Exception as e:
        logging.error(f"Erro ao inserir no banco: {e}")

if __name__ == "__main__":
    logging.info("")
    logging.info("Inicio do processo de insercao de registros no PostgreSQL.")
    insert_psql()
    logging.info("Fim do processo de insercao de registros no PostgreSQL.")