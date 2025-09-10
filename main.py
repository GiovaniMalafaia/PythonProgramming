import os
from datetime import datetime
import logging
import subprocess

# Caminho base do projeto
caminho = os.path.dirname(os.path.abspath(__file__))

# Configuracao de logging centralizado
data_atual = datetime.today().strftime("%Y-%m-%d")
log_file = os.path.join(caminho, f"logs\\{data_atual}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Adiciona linha em branco para separar execuções
logging.info("")
logging.info("Inicio da execucao do pipeline completo")

extrair = os.path.join(caminho, "etl\\Extract.py")
transformar = os.path.join(caminho, "etl\\Transform.py")
carregar = os.path.join(caminho, "etl\\Load.py")
carregarPsql = os.path.join(caminho, "etl\\LoadPostgreSQL.py")
insights = os.path.join(caminho, "etl\\InsightsAI.py")

pipeline = [extrair, transformar, carregar, carregarPsql, insights]

for etl in pipeline:
    try:
        arquivo = os.path.splitext(os.path.basename(etl))[0]
        print(f"\nIniciando arquivo {arquivo}")
        logging.info(f"Iniciando arquivo {arquivo}")

        subprocess.run(['python', etl], check=True)

        print(f"Arquivo {arquivo} finalizado")
        logging.info(f"Arquivo {arquivo} finalizado")

    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao rodar o arquivo {arquivo}")

logging.info("Fim da execucao do pipeline completo")