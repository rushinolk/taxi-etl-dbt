import kagglehub
import os
import shutil
from pathlib import Path

def run_extract(dataset_handle: str, bronze_dir: str):

    print(f"🚀 Iniciando extração do Kaggle: {dataset_handle}")

  
    os.makedirs(bronze_dir, exist_ok=True)


    temp_path = kagglehub.dataset_download(dataset_handle)
    
    print(f"✅ Download concluído no cache: {temp_path}")


    for file_name in os.listdir(temp_path):
        source = os.path.join(temp_path, file_name)
        destination = os.path.join(bronze_dir, file_name)

        shutil.copyfile(source, destination) # Copia ESTRITAMENTE os dados, ignorando permissões do SO
        os.remove(source)
        print(f"📦 Arquivo {file_name} movido para Bronze.")



if __name__ == "__main__":
    root_project = Path(__file__).parent.parent.parent
    bronze_dir = os.path.join(root_project,"include", "data", "bronze")
    
    handle = "elemento/nyc-yellow-taxi-trip-data"
    run_extract(handle,bronze_dir)