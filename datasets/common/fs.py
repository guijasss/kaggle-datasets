from pathlib import Path
from shutil import rmtree


def makedir(dirname: str) -> None:
    """
    Cria um diretório no diretório atual de execução, se não existir.

    Parâmetros:
    nome_diretorio (str): Nome do diretório a ser criado.

    Retorna:
    Path: Caminho absoluto do diretório criado/existente.
    """
    dir_path = Path.cwd() / dirname
    if not dir_path.exists():
        print(f"📁 Criando diretório '{dirname}' em {dir_path}")
        dir_path.mkdir()
    else:
        print(f"✅ Diretório '{dirname}' já existe em {dir_path}")


def rmdir(dirname: str) -> None:
    """
    Remove um diretório no diretório atual de execução, se ele existir.

    Parâmetros:
        nome_diretorio (str): Nome do diretório a ser removido.
    """
    dir_path = Path.cwd() / dirname
    if dir_path.exists():
        print(f"🧹 Removendo diretório '{dirname}' de {dir_path}")
        rmtree(dir_path)
    else:
        print(f"ℹ️ Diretório '{dirname}' não existe em {dir_path}")
