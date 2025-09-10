import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
import logging

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")
db = os.getenv("db")

conn_str = f"postgresql+psycopg2://{user}:{password}@ep-still-frog-ad9qpe84-pooler.c-2.us-east-1.aws.neon.tech/{db}?sslmode=require"
engine = create_engine(conn_str)

api_key = os.getenv("openai_api_key")
openai_client = OpenAI(api_key=api_key)

# Logging
caminho = os.path.dirname(os.path.abspath(__file__))
data_atual = datetime.today().strftime("%Y-%m-%d")
log_file = os.path.join(caminho, f"gold\\logs\\insights_{data_atual}.log")
output_file = os.path.join(caminho, f"gold\\insight_{data_atual}.txt")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def generate_response(openai_client, system_prompt_text, user_prompt_text):
    try:
        resposta = openai_client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": system_prompt_text},
                {"role": "user", "content": user_prompt_text}
            ],
            temperature=0.6,
        )
        return resposta.choices[0].message.content
    
    except Exception as e:
        logging.error(f"Erro ao gerar resposta com LLM: {e}")
        return None

def gerar_insights():
    try:
        df = pd.read_sql("SELECT * FROM public.taxa_cambio ORDER BY data_atualizacao DESC LIMIT 100", engine)
        if df.empty:
            logging.warning("Nenhum dado encontrado na tabela taxa_cambio")
            return

        logging.info(f"{len(df)} registros carregados para análise.")

        top5 = df.sort_values("taxa", ascending=False).head(5)
        resumo = top5[["moeda", "taxa", "base_currency"]].to_dict(orient="records")

        user_prompt_text = f"""
        Você é um analista financeiro. Explique de forma simples e executiva a situação das 5 principais moedas em relação ao Real hoje.
        Dados: {resumo}
        """

        system_prompt_text = "Você é um assistente prestativo que gera insights financeiros em linguagem clara e objetiva."

        insight = generate_response(openai_client, system_prompt_text, user_prompt_text)

        if insight:
            print("=== Insight do dia ===")
            print(insight)
            logging.info("Insight gerado com sucesso.")

            with open(output_file, "a", encoding="utf-8") as f:
                f.write(f"\n\n=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                f.write(insight)
            logging.info(f"Insight salvo em arquivo: {output_file}")
        else:
            logging.error("Falha ao gerar insight.")

    except Exception as e:
        logging.error(f"Erro no processo de geração de insights: {e}")

if __name__ == "__main__":
    logging.info("Início do processo de geração de insights com LLM")
    gerar_insights()
    logging.info("Fim do processo de geração de insights com LLM")