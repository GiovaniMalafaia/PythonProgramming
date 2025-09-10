# 🌍 Pipeline de ETL e Insights de Cotações de Moedas

Este projeto implementa um **pipeline de ETL (Extract, Transform, Load)** para coletar cotações de moedas via API, transformar os dados em camadas estruturadas e carregá-los em banco de dados PostgreSQL (Neon).  
Além disso, utiliza **LLM (ChatGPT)** para gerar **insights executivos** sobre a variação das principais moedas.

---

## 🚀 Estrutura do Projeto
```text
├── etl/
│ ├── Extract.py # Extrai dados da API e salva em /raw/
│ ├── Transform.py # Normaliza os dados e grava em /silver/
│ ├── Load.py # Converte para Parquet e salva em /gold/
│ ├── LoadPostgreSQL.py # Carrega dados para o PostgreSQL
│ ├── InsightsAI.py # Gera insights executivos via LLM
├── logs/ # Logs centralizados de execução
├── gold/ # Dados finais em Parquet
├── silver/ # Dados tratados
├── raw/ # Dados brutos da API
├── main.py # Orquestração do pipeline completo
├── .env # Variáveis de ambiente (chaves, credenciais)
└── README.md # Este documento
```
---

## 🛠️ Tecnologias Utilizadas

- **Python 3.12**
- **Bibliotecas**:  
  -  `os`, `requests`, `json`, `pandas`, `sqlalchemy`, `python-dotenv`, `logging`  
- **Banco de Dados**: PostgreSQL (Neon)  
- **LLM**: OpenAI GPT (para geração de insights executivos)  

---

## 🔄 Fluxo do Pipeline

1. **Extract** → Coleta as cotações da API e salva em `/raw/`.  
2. **Transform** → Normaliza os dados (datas, moedas, taxas) e grava em `/silver/`.  
3. **Load** → Converte os dados para formato Parquet e salva em `/gold/`.  
4. **LoadPostgreSQL** → Carrega os dados finais no banco Neon/PostgreSQL.  
5. **InsightsAI** → Utiliza ChatGPT para gerar análises executivas e salvar em `.txt`.  

---

## 📥 Como baixar o projeto

Clone o repositório com:

```bash
git clone https://github.com/GiovaniMalafaia/PythonProgramming.git
```
---

## ▶️ Como Executar

Na raiz do projeto, basta rodar:

```bash
python main.py
```

Isso executa todas as etapas do pipeline em sequência: Extract → Transform → Load → LoadPostgreSQL → InsightsAI.

Os logs são gravados em logs/YYYY-MM-DD.log.
```

## 📌 Objetivo Acadêmico

Este projeto foi desenvolvido como parte do Trabalho de Faculdade em Engenharia de Dados, com foco em:

- Programação em Python;

- Boas práticas de ETL;

- Uso de camadas de dados (Raw, Silver, Gold);

- Integração com banco relacional;

- Enriquecimento de dados com Inteligência Artificial (LLM).