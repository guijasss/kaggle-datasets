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
        ],
        "subtitle": "A realistic dataset based on the VERIS Community Database (VCDB)",
        "description": "This dataset is a processed version of the VERIS Community Database (VCDB), structured for usability in data analysis and machine learning workflows. It includes a cleaned and normalized version of incident records, focused on cybersecurity events. Ideal for anomaly detection, data modeling, and research in security incidents.\n\n**Contents**:\n- Multiple CSV files containing incident-level details\n- Pre-processed attributes (action, actor, asset, etc.)\n\n**Source**: Based on the VCDB project (https://vcdb.org/)\n\n**Notes**:\n- This version is structured for use with Polars or Pandas\n- Nulls and nested fields were flattened\n\nContributions welcome via GitHub: https://github.com/guijas/kaggle-datasets",
        "isPrivate": False,
        "keywords": ["cybersecurity", "security", "vcdb", "veris", "incidents", "dataset", "breach"],
        "data": [],
        "collaborators": []
    }

    with open("/tmp/kaggle/dataset-metadata.json", "w", encoding="utf-8") as f:
        dump(dataset_metadata, f, indent=2, ensure_ascii=False)

    # Cria um dataset se não existir
    api.dataset_create_version(
        folder=dataset_path,
        version_notes="Atualização dos metadados via script",
        delete_old_versions=False
    )
