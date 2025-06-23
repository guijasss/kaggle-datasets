import requests
import os
from datetime import datetime
import zipfile
import io

# Configurações
SOURCES = [
    "https://github.com/vz-risk/VCDB/raw/master/data/csv/vcdb.csv.zip"
]

OUTPUT_DIR = "../data"
OUTPUT_CSV = f"{OUTPUT_DIR}/vcdb.csv"


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
                # Lista os arquivos dentro do zip
                file_names = z.namelist()

                # Encontrar o primeiro arquivo CSV dentro do ZIP
                csv_files = [f for f in file_names if f.lower().endswith(".csv")]

                if not csv_files:
                    print("Nenhum arquivo CSV encontrado no zip.")
                    return False

                csv_name = csv_files[0]
                print(f"Extraindo {csv_name}...")

                with z.open(csv_name) as f_in, open(OUTPUT_CSV, 'wb') as f_out:
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
