import os
import json
from datetime import datetime
import logging

# Caminho base do projeto
caminho = os.path.dirname(os.path.abspath(__file__))

# Pastas
pasta_raw = os.path.join(caminho, "raw")
pasta_silver = os.path.join(caminho, "silver")
os.makedirs(pasta_silver, exist_ok=True)

# Configuracao do logging
data_atual = datetime.today().strftime("%Y-%m-%d")
log_file = os.path.join(pasta_silver, f"logs\\{data_atual}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def obter_ultimo_arquivo_raw():
    logging.info(f"Obtendo ultimo arquivo da pasta RAW...")
    arquivos = [f for f in os.listdir(pasta_raw) if f.endswith(".json")]
    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta /raw/")
    arquivos.sort()
    return os.path.join(pasta_raw, arquivos[-1])

def normalizar_data(data_str):
    logging.info(f"Realizando normalizacao das datas...")
    try:
        dt = datetime.strptime(data_str, "%a, %d %b %Y %H:%M:%S %z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def transformar_taxas():
    try:
        arquivo_raw = obter_ultimo_arquivo_raw()
        logging.info(f"Usando arquivo RAW: {arquivo_raw}")

        # Nome do arquivo silver baseado na data de hoje
        arquivo_silver = os.path.join(pasta_silver, f"{data_atual}.json")

        # Se já existir, deleta
        if os.path.exists(arquivo_silver):
            os.remove(arquivo_silver)
            logging.info(f"Arquivo existente removido: {arquivo_silver}")

        with open(arquivo_raw, "r", encoding="utf-8") as f:
            dados_raw = json.load(f)

        logging.info(f"Fazendo tratamento dos registros...")

        base_currency = dados_raw.get("base_code")
        timestamp = dados_raw.get("time_last_update_unix")
        last_update_utc = normalizar_data(dados_raw.get("time_last_update_utc", ""))
        next_update_utc = normalizar_data(dados_raw.get("time_next_update_utc", ""))
        conversion_rates = dados_raw.get("conversion_rates", {})

        registros = []
        for moeda, taxa in conversion_rates.items():
            if taxa is None or taxa <= 0:
                continue
            
            registros.append({
                "moeda": moeda,
                "taxa": taxa,
                "base_currency": base_currency,
                "timestamp": timestamp,
                "time_last_update_utc": last_update_utc,
                "time_next_update_utc": next_update_utc
            })

        with open(arquivo_silver, "w", encoding="utf-8") as f:
            json.dump(registros, f, indent=4, ensure_ascii=False)

        logging.info(f"Transformacao concluida. Arquivo salvo em: {arquivo_silver}")

    except Exception as e:
        logging.error(f"Erro na transformacao: {e}")

if __name__ == "__main__":
    logging.info("")
    logging.info("Inicio do processo de transformacao das taxas.")
    transformar_taxas()
    logging.info("Fim do processo de transformacao das taxas.")