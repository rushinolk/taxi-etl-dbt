# 🚕 NYC Yellow Taxi: End-to-End Data Engineering Pipeline

Pipeline de dados **ELT** construído para extrair, processar e gerar inteligência a partir do dataset massivo de táxis de Nova York (TLC). Da ingestão de dados brutos à visualização em um painel executivo.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)

---

## 📊 Resultados em Destaque

| Métrica | Resultado |
| :--- | :--- |
| 🗂️ **Registros Processados** | **33.86M corridas** validadas e modeladas na camada Gold |
| 💰 **Receita Analisada** | **$ 531.22 Milhões** de faturamento rastreado no período |
| 💳 **Ticket Médio** | **$ 15.39** cobrados por corrida |
| 🏢 **Market Share** | **VeriFone Inc (53.7%)** vs Creative Mobile (46.3%) |
| 🕕 **Pico de Demanda** | **18h – 20h** concentra o maior volume de viagens |
| ⚡ **Performance OLAP** | **Segundos** para processar 33M+ de linhas (DuckDB in-process) |

---

## 🏗️ Arquitetura do Pipeline (Medallion)

```text
  KAGGLE API (Dataset TLC)
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│                    CAMADA BRONZE  (Python)                   │
│  Extração bruta ──► nyc_yellow_taxi_raw.parquet              │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│        ORQUESTRAÇÃO: APACHE AIRFLOW + ASTRONOMER COSMOS      │
│  Isolamento de concorrência e execução de DAGs dinâmicas     │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│             CAMADA SILVER / INTERMEDIATE  (dbt)              │
│  stg_taxi ──► Padronização (snake_case) e tipagem            │
│  int_taxi_flagged ──► Limpeza e validação escalar            │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│             CAMADA GOLD / MARTS  (dbt + DuckDB)              │
│  mart_taxi_metrics ──► Agregações OLAP (Sazonalidade, KPIs)  │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│                  DASHBOARD  (Streamlit)                      │
│       Frontend desacoplado consumindo os dados em Parquet    │
└──────────────────────────────────────────────────────────────┘



## 🛠️ Stack Tecnológica

| Componente | Ferramenta | Justificativa |
| :--- | :--- | :--- |
| **Ingestão** | `Python` | Download massivo via API do Kaggle para armazenamento local. |
| **Armazenamento** | `Apache Parquet` | Formato colunar otimizado para leitura e alta compressão. |
| **Processamento** | `DuckDB` | OLAP in-process. Consultas em milhões de linhas com altíssima performance local (sem custos cloud). |
| **Transformação** | `dbt Core` | Modelagem SQL-first, garantindo linhagem de dados e aplicação de regras de negócio. |
| **Orquestração** | `Airflow + Cosmos` | O Cosmos traduz os modelos do dbt nativamente em Tasks do Airflow para automação visual. |
| **Dashboard** | `Streamlit` | Frontend Python interativo lendo diretamente a camada Gold. |

---

## 💡 Soluções de Engenharia (Destaques)

* **Prevenção de OOM (Out of Memory):** Substituição de funções de janela complexas por processamento escalar lógico (`CASE WHEN`), transferindo o gargalo da memória RAM para a CPU.
* **Gestão de Locks no DuckDB:** Implementação de limite de concorrência (`max_active_tasks=1` e `threads: 1`) para evitar *Deadlocks* de escrita no banco de arquivo único.
* **Decoupled Architecture:** O Streamlit opera de forma "cega", lendo os arquivos consolidados sem depender do dbt ou do Airflow estarem online.

---

## 🚀 Como Executar Localmente

**Pré-requisitos:** Docker Desktop e Astro CLI.

1. **Clone o repositório:**
   ```bash
   git clone [https://github.com/Arthurmgomes/nyc-yellow-taxi-pipeline.git](https://github.com/Arthurmgomes/nyc-yellow-taxi-pipeline.git)
   cd nyc-yellow-taxi-pipeline
   ```

2. **Inicie a Orquestração (Airflow):**
   ```bash
   astro dev start
   ```

3. **Execute o Pipeline:**
   Acesse `http://localhost:8080` (usuário: `admin` / senha: `admin`), ative a DAG `nyc_taxi_end_to_end_cosmos` e aguarde a finalização (status verde).

4. **Abra o Painel Executivo:**
   Em um novo terminal, inicie a aplicação Streamlit:
   ```bash
   streamlit run dashboard.py
   ```

---

## 📸 Screenshots

*Substitua os links abaixo pelas suas imagens:*

![Airflow DAG](LINK_DA_IMAGEM_AQUI)

![Dashboard Overview](LINK_DA_IMAGEM_AQUI)

---
### 👨‍💻 Autor
**Arthur Machado Gomes**
* [LinkedIn](https://www.linkedin.com/in/arthur-gomes1/)
* [GitHub](https://github.com/Arthurmgomes)