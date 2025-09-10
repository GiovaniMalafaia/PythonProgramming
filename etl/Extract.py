import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

# Configuração do logging
caminho = os.path.dirname(os.path.abspath(__file__))
pasta_raw = os.path.join(caminho, "raw")
os.makedirs(pasta_raw, exist_ok=True)

data_atual = datetime.today().strftime("%Y-%m-%d")
nome_arquivo = os.path.join(pasta_raw, f"{data_atual}.json")

log_file = os.path.join(pasta_raw, f"logs\\{data_atual}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

load_dotenv()

chave_api = os.getenv("chave_api")
moeda_base = os.getenv("moeda_base")

url = f"https://v6.exchangerate-api.com/v6/{chave_api}/latest/{moeda_base}"

def coletar_taxas():
    try:
        if os.path.exists(nome_arquivo):
            os.remove(nome_arquivo)
            logging.info(f"Arquivo existente removido: {nome_arquivo}")

        logging.info(f"Iniciando requisicao para URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Requisicao bem-sucedida.")

        dados = response.json()

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)

        logging.info(f"Arquivo '{nome_arquivo}' salvo com sucesso!")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao coletar taxas: {e}")

    except Exception as e:
        logging.error(f"Erro inesperado: {e}")

if __name__ == "__main__":
    logging.info("")
    logging.info("Inicio do processo de coleta de taxas.")
    coletar_taxas()
    logging.info("Fim do processo de coleta de taxas.")