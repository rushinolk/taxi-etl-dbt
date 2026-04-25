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


## 📐 Arquitetura Medallion (Bronze → Silver → Gold)

Todo o fluxo de dados foi modelado seguindo as melhores práticas de Analytics Engineering:

* 🥉 **Camada Bronze (Ingestão)**
  * **Processo:** Download massivo direto via API oficial do dataset.
  * **Resultado:** Dados brutos intocados, salvos no formato `.parquet` para redução drástica do armazenamento local (Landing Zone).

* 🥈 **Camada Silver (Limpeza e Validação)**
  * **Processo (dbt):** Transformação em duas etapas (`staging` e `intermediate`).
  * **Resultado:** Padronização de nomenclatura (`snake_case`), casting de tipos de dados (timestamp, numéricos) e aplicação de regras de negócio. Eliminação de anomalias financeiras (ex: tarifas negativas) e corridas com coordenadas geográficas inválidas ou nulas.

* 🥇 **Camada Gold (Analytics & Marts)**
  * **Processo (dbt + DuckDB):** Agregações dimensionais para o usuário final de negócios.
  * **Resultado:** Tabela `mart_taxi_metrics` consolidando a performance operacional por hora, participação de mercado das provedoras de taxímetro e métricas de pagamento prontas para o consumo do Dashboard.

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

```

## 🛠️ Stack Tecnológica

| Componente | Ferramenta |
| :--- | :--- |
| **Ingestão** | `Python` |
| **Armazenamento** | `Parquet` |
| **Processamento** | `DuckDB` |
| **Transformação** | `dbt Core` |
| **Orquestração** | `Airflow + Cosmos` |
| **Dashboard** | `Streamlit` |

---

## 💡 Soluções de Engenharia (Destaques)

* **dbt + DuckDB para Big Data Local:** A combinação dessas ferramentas foi a escolha arquitetural principal devido ao tamanho massivo dos dados (+11.3 milhões de registros). O DuckDB atua como um motor analítico hiper-otimizado que mastiga esse volume em segundos na própria máquina, enquanto o dbt gerencia as transformações com SQL limpo e versionado.
* **Arquitetura 100% Desacoplada:** Todo o fluxo foi desenhado com separação de responsabilidades. A ingestão em Python ocorre separada da transformação, e o painel no Streamlit opera de forma totalmente independente (ele consome apenas os arquivos `.parquet` finais da camada Gold, sem precisar que o Airflow ou o banco de dados estejam ativos durante a navegação).
* **Prevenção de OOM e Lógica Escalar:** Para evitar o esgotamento de memória RAM (OOM Killer do Docker) ao processar todas as linhas simultaneamente, substituímos funções globais de ordenação em memória por **processamento escalar lógico** (`CASE WHEN`), transferindo a carga para a CPU e garantindo estabilidade no pipeline.

---

## 🚀 Como Executar Localmente

**Pré-requisitos:** Docker Desktop e Astro CLI.

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/rushinolk/nyc-yellow-taxi-pipeline.git
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
* [GitHub](https://github.com/rushinolk)