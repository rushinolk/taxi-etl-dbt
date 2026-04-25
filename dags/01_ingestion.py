from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

# 1. Importando a sua função de extração
# Ajuste 'src.scripts' para o nome exato da pasta onde está o seu extract.py
from src import extract

# Importações do Cosmos
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode

# Caminhos Básicos
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DBT_PROJECT_DIR = Path(f"{AIRFLOW_HOME}/include/dbt")

# Configuração do Perfil (Dizendo ao Cosmos onde está o profiles.yml do DuckDB)
profile_config = ProfileConfig(
    profile_name="taxi_pipeline", 
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml"
)

default_args = {
    "depends_on_past" : False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

# 2. Abrindo a DAG com a sintaxe 'with'
with DAG(
    dag_id="nyc_taxi_end_to_end_cosmos",
    description="Pipeline Completo com Extração e dbt via Cosmos",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc_taxi", "duckdb", "cosmos"],
) as dag:

    # ---------------------------------------------------------
    # TAREFA 1: Extração (PythonOperator)
    # ---------------------------------------------------------
    def executar_extracao():
        """Função wrapper para passar os parâmetros para o seu script"""
        caminho_bronze = Path(f"{AIRFLOW_HOME}/include/data/bronze")
        repo_kaggle = "elemento/nyc-yellow-taxi-trip-data"
        
        # Chama a função que você importou lá em cima
        extract.run_extract(repo_kaggle, caminho_bronze)
        print("Extração concluída com sucesso!")

    task_extract = PythonOperator(
        task_id="extract_kaggle_data",
        python_callable=executar_extracao
    )

    # ---------------------------------------------------------
    # TAREFA 2: Transformação dbt (Cosmos DbtTaskGroup)
    # ---------------------------------------------------------
    task_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            # Removido o dbt_executable_path. 
            # O Cosmos vai usar o dbt padrão instalado no container.
        ),
    )

    # ---------------------------------------------------------
    # ORDEM DE EXECUÇÃO
    # ---------------------------------------------------------
    task_extract >> task_transform