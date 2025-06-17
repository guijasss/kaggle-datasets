import polars as pl
import glob
import os
import psutil
import sys
import gc

# Limite de RAM em bytes (2 GB)
RAM_LIMIT = 2 * 1024 ** 3


def check_ram_usage():
    process = psutil.Process(os.getpid())
    mem_usage = process.memory_info().rss
    if mem_usage > RAM_LIMIT:
        print(f"❌ RAM excedida: {mem_usage / 1024 ** 3:.2f} GB usados.")
        sys.exit("Encerrando por uso excessivo de memória.")
    else:
        print(f"✅ RAM OK: {mem_usage / 1024 ** 3:.2f} GB")
    return mem_usage


def limpar_memoria():
    gc.collect()


def criar_base_ids_unicos(arquivos, chave_join):
    """Cria um DataFrame base com todos os IDs únicos de todos os arquivos"""

    print("🔍 Coletando todos os IDs únicos...")

    todos_ids = set()

    for i, arquivo in enumerate(arquivos):
        nome = os.path.basename(arquivo)
        print(f"   📄 Lendo IDs de {nome}...")

        # Lê apenas a coluna de ID
        df_ids = pl.read_parquet(arquivo).select(chave_join)
        ids_unicos = df_ids[chave_join].unique().to_list()

        print(f"      📊 {len(ids_unicos)} IDs únicos")
        todos_ids.update(ids_unicos)

        del df_ids
        limpar_memoria()
        check_ram_usage()

    print(f"\n🎯 Total de IDs únicos em todos os arquivos: {len(todos_ids)}")

    # Cria DataFrame base com IDs únicos
    df_base = pl.DataFrame({chave_join: list(todos_ids)})

    return df_base


def analisar_sobreposicao_ids(arquivos, chave_join):
    """Analisa a sobreposição de IDs entre arquivos"""

    print("🔍 ANÁLISE DE SOBREPOSIÇÃO DE IDs")
    print("=" * 50)

    # Coleta IDs de cada arquivo
    ids_por_arquivo = {}

    for arquivo in arquivos:
        nome = os.path.basename(arquivo)
        print(f"📄 Analisando {nome}...")

        df = pl.read_parquet(arquivo)
        ids_unicos = set(df[chave_join].unique().to_list())

        ids_por_arquivo[nome] = {
            'ids': ids_unicos,
            'count': len(ids_unicos),
            'total_rows': len(df)
        }

        print(f"   📊 IDs únicos: {len(ids_unicos)}")
        print(f"   📊 Total linhas: {len(df)}")
        print(f"   📊 IDs duplicados no arquivo: {len(df) - len(ids_unicos)}")

        del df
        limpar_memoria()

    # Analisa sobreposição
    print(f"\n🔍 ANÁLISE DE SOBREPOSIÇÃO:")
    nomes_arquivos = list(ids_por_arquivo.keys())

    for i, nome1 in enumerate(nomes_arquivos):
        for nome2 in nomes_arquivos[i + 1:]:
            ids1 = ids_por_arquivo[nome1]['ids']
            ids2 = ids_por_arquivo[nome2]['ids']

            intersecao = ids1.intersection(ids2)
            uniao = ids1.union(ids2)

            print(f"   🔗 {nome1} ∩ {nome2}:")
            print(f"      🎯 IDs em comum: {len(intersecao)}")
            print(f"      📊 Total únicos: {len(uniao)}")
            print(f"      📈 Sobreposição: {len(intersecao) / len(uniao) * 100:.1f}%")

    return ids_por_arquivo


def join_controlado_com_base(arquivos, chave_join, usar_intersecao=True):
    """Join controlado usando base de IDs"""

    print("🎯 JOIN CONTROLADO COM BASE DE IDs")

    # Primeira opção: analisa sobreposição
    print("\n🔍 Analisando sobreposição...")
    ids_info = analisar_sobreposicao_ids(arquivos, chave_join)

    if usar_intersecao:
        # Usa apenas IDs que existem em TODOS os arquivos
        print("\n🎯 Estratégia: INTERSEÇÃO (IDs que existem em TODOS os arquivos)")

        # Encontra IDs comuns a todos
        ids_comuns = None
        for nome, info in ids_info.items():
            if ids_comuns is None:
                ids_comuns = info['ids'].copy()
            else:
                ids_comuns = ids_comuns.intersection(info['ids'])

        print(f"   🎯 IDs comuns a todos: {len(ids_comuns)}")

        if len(ids_comuns) == 0:
            print("❌ Nenhum ID comum entre todos os arquivos!")
            return None

        # Cria base com IDs comuns
        df_base = pl.DataFrame({chave_join: list(ids_comuns)})

    else:
        # Usa todos os IDs únicos
        print("\n🎯 Estratégia: UNIÃO (todos os IDs únicos)")
        df_base = criar_base_ids_unicos(arquivos, chave_join)

    print(f"   📊 Base criada: {df_base.shape}")
    check_ram_usage()

    # Agora faz join controlado
    df_resultado = df_base

    for i, arquivo in enumerate(arquivos):
        nome = os.path.basename(arquivo)
        print(f"\n🔗 Join {i + 1}/{len(arquivos)}: {nome}")

        # Carrega arquivo
        df_temp = pl.read_parquet(arquivo)
        print(f"   📄 Arquivo original: {df_temp.shape}")

        # Remove duplicatas por ID (fica apenas com o primeiro)
        df_temp_unique = df_temp.unique(subset=[chave_join], keep="first")
        print(f"   📄 Após remoção de duplicatas: {df_temp_unique.shape}")

        # Identifica colunas novas
        colunas_novas = [c for c in df_temp_unique.columns if c not in df_resultado.columns]

        if colunas_novas:
            print(f"   📋 Colunas novas: {colunas_novas}")

            # Seleciona apenas colunas novas + chave
            colunas_necessarias = [chave_join] + colunas_novas
            df_temp_select = df_temp_unique.select(colunas_necessarias)

            # LEFT JOIN (mantém todos os IDs da base)
            df_resultado = df_resultado.join(df_temp_select, on=chave_join, how="left")
            print(f"   📊 Resultado: {df_resultado.shape}")

            # VERIFICAÇÃO CRÍTICA: Se o número de linhas mudou, ALGO ESTÁ ERRADO!
            if df_resultado.height != df_base.height:
                print(f"   ❌ ERRO! Linhas mudaram: {df_base.height} → {df_resultado.height}")
                print("   🛑 Parando para evitar explosão de memória!")
                break

        else:
            print(f"   ⚠️  Nenhuma coluna nova em {nome}")

        del df_temp, df_temp_unique
        if 'df_temp_select' in locals():
            del df_temp_select
        limpar_memoria()
        check_ram_usage()

    return df_resultado


def estrategia_arquivo_principal(arquivos, chave_join, arquivo_principal=None):
    """Usa um arquivo específico como base"""

    print("🎯 ESTRATÉGIA: ARQUIVO PRINCIPAL COMO BASE")

    # Escolhe arquivo principal
    if arquivo_principal:
        arquivo_base = None
        for arq in arquivos:
            if arquivo_principal in os.path.basename(arq):
                arquivo_base = arq
                break

        if not arquivo_base:
            print(f"❌ Arquivo '{arquivo_principal}' não encontrado!")
            return None
    else:
        # Usa o primeiro arquivo
        arquivo_base = arquivos[0]

    print(f"🟢 Arquivo base: {os.path.basename(arquivo_base)}")

    # Carrega arquivo base
    df_resultado = pl.read_parquet(arquivo_base)
    print(f"   📊 Base: {df_resultado.shape}")

    # Remove duplicatas da base
    df_resultado = df_resultado.unique(subset=[chave_join], keep="first")
    print(f"   📊 Base sem duplicatas: {df_resultado.shape}")
    check_ram_usage()

    # Join com outros arquivos
    outros_arquivos = [arq for arq in arquivos if arq != arquivo_base]

    for i, arquivo in enumerate(outros_arquivos):
        nome = os.path.basename(arquivo)
        print(f"\n🔗 Join {i + 1}/{len(outros_arquivos)}: {nome}")

        df_temp = pl.read_parquet(arquivo)
        print(f"   📄 Arquivo: {df_temp.shape}")

        # Remove duplicatas
        df_temp = df_temp.unique(subset=[chave_join], keep="first")
        print(f"   📄 Sem duplicatas: {df_temp.shape}")

        # Colunas novas
        colunas_novas = [c for c in df_temp.columns if c not in df_resultado.columns]

        if colunas_novas:
            df_temp = df_temp.select([chave_join] + colunas_novas)
            print(f"   📋 Selecionadas: {len(colunas_novas)} colunas")

            # LEFT JOIN
            linhas_antes = df_resultado.height
            df_resultado = df_resultado.join(df_temp, on=chave_join, how="left")

            print(f"   📊 Resultado: {df_resultado.shape}")

            # Verificação crítica
            if df_resultado.height != linhas_antes:
                print(f"   ❌ ERRO! Linhas mudaram: {linhas_antes} → {df_resultado.height}")
                print("   🛑 Interrompendo!")
                break

        del df_temp
        limpar_memoria()
        check_ram_usage()

    return df_resultado


# EXECUÇÃO PRINCIPAL
if __name__ == "__main__":
    caminho = "./_tmp"
    chave_join = "incident_id"
    arquivos = glob.glob(os.path.join(caminho, "*.parquet"))

    if len(arquivos) < 2:
        raise Exception("É necessário pelo menos dois arquivos Parquet.")

    print(f"📁 Encontrados {len(arquivos)} arquivos")
    print("🎯 Problema identificado: PRODUTO CARTESIANO nos joins!")
    print("💡 Soluções disponíveis:\n")

    try:
        # Escolha da estratégia
        print("🎯 Escolhendo estratégia automaticamente...")

        if len(arquivos) <= 5:
            print("📊 Usando: INTERSECÇÃO (IDs comuns a todos)")
            df_final = join_controlado_com_base(arquivos, chave_join, usar_intersecao=True)
        else:
            print("📊 Usando: ARQUIVO PRINCIPAL como base")
            df_final = estrategia_arquivo_principal(arquivos, chave_join)

        if df_final is not None:
            print(f"\n✅ Join controlado completo!")
            print(f"📊 Resultado final: {df_final.shape}")

            # Salva resultado
            df_final.write_csv("resultado_controlado.csv")
            print("💾 Salvo: resultado_controlado.parquet")

            # Preview
            print("\n👀 Preview:")
            print(df_final.head())

            # Estatísticas
            print(f"\n📈 Estatísticas:")
            print(f"   🎯 Total de linhas: {df_final.height:,}")
            print(f"   📋 Total de colunas: {df_final.width}")
            print(f"   🔍 IDs únicos: {df_final[chave_join].n_unique()}")

            del df_final
            limpar_memoria()
            check_ram_usage()

    except Exception as e:
        print(f"❌ Erro: {e}")
        check_ram_usage()

    # Limpeza
    temp_files = glob.glob("checkpoint_*.parquet")
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)