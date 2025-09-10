### 📌 Pipeline a ser executado

```markdown
# 📂 Scripts ETL

Esta pasta contém os scripts responsáveis pelo **processamento dos dados em camadas**.

---

## 📜 Scripts

- **Extract.py**
  - Consome a API de câmbio.
  - Salva o resultado bruto em `/raw/` em formato `.json`.

- **Transform.py**
  - Lê os dados brutos de `/raw/`.
  - Normaliza campos de data e taxa de câmbio.
  - Gera uma versão tratada em `/silver/`.

- **Load.py**
  - Converte os dados de `/silver/` em `.parquet`.
  - Salva em `/gold/`.

- **LoadPostgreSQL.py**
  - Conecta ao banco de dados PostgreSQL (Neon).
  - Insere os dados da camada `gold` na tabela `public.taxa_cambio`.
  - Acrescenta coluna `data_atualizacao`.

- **InsightsAI.py**
  - Lê os dados do PostgreSQL.
  - Passa os dados para o ChatGPT (LLM).
  - Gera **insights em linguagem natural**.
  - Salva em arquivo `.txt` com timestamp.
```
---

## 🔎 Estrutura das Camadas

- `/raw/` → Dados brutos da API  
- `/silver/` → Dados tratados e padronizados  
- `/gold/` → Dados finais em formato Parquet  
- PostgreSQL → Armazenamento para análise  
- Insights (txt) → Explicações executivas para usuários de negócio  

---

## 📊 Dashboard Streamlit

Após rodar todo o pipeline, você pode visualizar os dados e gráficos de taxa de câmbio usando **Streamlit**.  
O dashboard lê diretamente do PostgreSQL e apresenta:

- Tabela interativa com os últimos registros.
- Gráficos de evolução das principais moedas.
- Resumos de insights do dia.

### Como executar:

1. Certifique-se de estar na pasta do projeto.
2. Rode o comando:

```bash
streamlit run dashboard.py