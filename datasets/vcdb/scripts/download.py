import requests
import os
from datetime import datetime
from pathlib import Path
import zipfile
import io

# Configurações
SOURCES = [
    "https://github.com/vz-risk/VCDB/raw/refs/heads/master/data/csv/vcdb.csv.zip"
]

OUTPUT_DIR = "/tmp/data"
OUTPUT_CSV = f"{OUTPUT_DIR}/vcdb.csv"
CSV_INTERNAL_PATH = "data/dbir/VCDB/data/csv/vcdb.csv"  # Caminho exato dentro do zip


def download_and_extract():
    """Tenta baixar de múltiplas fontes e extrair o CSV"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for url in SOURCES:
        try:
            print(f"Tentando {url}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            print("Download bem-sucedido! Extraindo conteúdo...")

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:

                if CSV_INTERNAL_PATH not in z.namelist():
                    print(f"{CSV_INTERNAL_PATH} não encontrado dentro do zip.")
                    return False

                print(f"Extraindo {CSV_INTERNAL_PATH}...")

                with z.open(CSV_INTERNAL_PATH) as f_in, open(OUTPUT_CSV, 'wb') as f_out:
                    f_out.write(f_in.read())

            print(f"Arquivo salvo em {OUTPUT_CSV}")
            return True

        except Exception as e:
            print(f"Falha na fonte {url}: {str(e)[:100]}...")

    return False


if __name__ == "__main__":
    print(f"Iniciando download em {datetime.now()}")

    if download_and_extract():
        print("Download e extração concluídos com sucesso!")
        exit(0)
    else:
        print("Todas as fontes falharam!")
        exit(1)
