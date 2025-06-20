from json import dump
import os
from kaggle.api.kaggle_api_extended import KaggleApi


def send_dataset():
    # Configurar o caminho da credencial
    os.environ['KAGGLE_CONFIG_DIR'] = "~/.config/kaggle"

    api = KaggleApi()
    api.authenticate()

    # Exemplo: enviar CSVs do diretório /tmp para um novo dataset
    dataset_path = "/tmp/kaggle"

    dataset_metadata = {
        "title": "VCDB Dataset",
        "id": "guijas/vcdb-dataset",
        "licenses": [
            {
                "name": "CC0-1.0"
            }
        ]
    }

    with open("/tmp/kaggle/dataset-metadata.json", "w", encoding="utf-8") as f:
        dump(dataset_metadata, f, indent=2, ensure_ascii=False)

    # Cria um dataset se não existir
    api.dataset_create_new(
        folder=dataset_path,
        public=True,
        convert_to_csv=False,
        dir_mode='skip'  # ignora se já existe metadata.json
    )
