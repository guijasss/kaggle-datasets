import requests
import os
from datetime import datetime
from pathlib import Path

# Configurações
SOURCES = [
    "https://raw.githubusercontent.com/example/vcdb/main/data/vcdb.csv",
    "https://backup.example.com/vcdb.csv"
]
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_FILE = OUTPUT_DIR / "vcdb_raw.csv"

def ensure_directory_exists():
    """Garante que o diretório de saída existe"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_with_fallback():
    """Tenta baixar de múltiplas fontes"""
    for url in SOURCES:
        try:
            print(f"Tentando {url}...")
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            with open(OUTPUT_FILE, 'wb') as f:
                f.write(response.content)
            print(f"Download bem-sucedido! Salvo como {OUTPUT_FILE}")
            return True
            
        except Exception as e:
            print(f"Falha na fonte {url}: {str(e)[:100]}...")
    
    return False

if __name__ == "__main__":
    print(f"Iniciando download em {datetime.now()}")
    ensure_directory_exists()
    
    if download_with_fallback():
        print("Download concluído com sucesso!")
        exit(0)
    else:
        print("Todas as fontes falharam!")
        exit(1)