import os
import json
import pandas as pd
from datetime import datetime
import logging

# Diretório base
caminho = os.path.dirname(os.path.abspath(__file__))

pasta_silver = os.path.join(caminho, "silver")
pasta_gold = os.path.join(caminho, "gold")
os.makedirs(pasta_gold, exist_ok=True)

# Data de hoje para padronizar nome
data_atual = datetime.today().strftime("%Y-%m-%d")
arquivo_gold = os.path.join(pasta_gold, f"{data_atual}.parquet")

# Configuracao do logging
log_file = os.path.join(pasta_gold, f"logs\\parquet_{data_atual}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def salvar_gold():
    try:
        logging.info(f"Listando arquivos da pasta SILVER...")
        arquivos_silver = sorted(
            [f for f in os.listdir(pasta_silver) if f.endswith(".json")]
        )
        if not arquivos_silver:
            logging.warning("Nenhum arquivo encontrado em /silver/")
            return

        # Pega o mais recente
        ultimo_arquivo = arquivos_silver[-1]
        caminho_arquivo = os.path.join(pasta_silver, ultimo_arquivo)
        logging.info(f"Usando arquivo SILVER: {caminho_arquivo}")

        # Carrega JSON
        with open(caminho_arquivo, "r", encoding="utf-8") as f:
            dados = json.load(f)

        # Converte para DataFrame
        logging.info(f"Convertendo arquivo json para um dataframe...")
        df = pd.DataFrame(dados)

        # Se já existir parquet do dia, remove
        if os.path.exists(arquivo_gold):
            os.remove(arquivo_gold)
            logging.info(f"Arquivo existente removido: {arquivo_gold}")

        # Salva em Parquet
        df.to_parquet(arquivo_gold, index=False, engine="pyarrow")
        logging.info(f"Arquivo salvo em: {arquivo_gold}")

    except Exception as e:
        logging.error(f"Erro ao salvar gold: {e}")

if __name__ == "__main__":
    logging.info("")
    logging.info("Inicio do processo de geracao Gold.")
    salvar_gold()
    logging.info("Fim do processo de geracao Gold.")