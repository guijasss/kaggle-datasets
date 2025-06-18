from pandas import DataFrame, Series, read_csv


def coalesce(df: DataFrame, cols: list[str]) -> Series:
    """
    Retorna uma Series com o primeiro valor não nulo entre as colunas especificadas.

    :param df: DataFrame contendo as colunas.
    :param cols: Lista de nomes de colunas para aplicar o coalesce.
    :return: Series com o primeiro valor não nulo por linha.
    """
    if not cols:
        raise ValueError("A lista de colunas não pode estar vazia.")

    result = df[cols[0]]
    for col in cols[1:]:
        result = result.combine_first(df[col])
    return result


def write_dataframe(df: DataFrame, name: str) -> None:
    df.to_csv(f"./_tmp/{name}.csv")
    
    
def read_columns_txt(path: str) -> list:
    return open(path, "r").read().splitlines()


def get_vcdb(columns: list = None) -> DataFrame:
    return read_csv(
            "./data/vcdb.csv",
            low_memory=False,
            header=0,
            usecols=columns or read_columns_txt(path="./docs/columns.txt")
            )


def filter_columns(include_patterns: list[str], exclude_patterns: list[str] = None):
    columns = read_columns_txt("./docs/columns.txt")

    def pattern_to_parts(pattern: str):
        return pattern.split(".")

    def match(col: str, parts: list[str]) -> bool:
        col_parts = col.split(".")
        if len(col_parts) < len(parts):
            return False
        return all(p == "*" or p == c for p, c in zip(parts, col_parts))

    # Incluir colunas que casam com qualquer padrão de inclusão
    included = []
    for pattern in include_patterns:
        parts = pattern_to_parts(pattern)
        included += [col for col in columns if match(col, parts)]

    included = list(set(included))  # remove duplicatas

    # Excluir colunas que casam com qualquer padrão de exclusão
    if exclude_patterns:
        for pattern in exclude_patterns:
            parts = pattern_to_parts(pattern)
            included = [col for col in included if not match(col, parts)]

    # Sempre incluir incident_id
    if "incident_id" not in included:
        included.append("incident_id")

    return included


def undo_onehot_encoding(df: DataFrame, prefix: str) -> Series:
    columns = [col for col in df.columns if col.startswith(prefix + ".")]
    def extract_value(row):
        for col in columns:
            if row[col]:
                return col.replace(prefix + ".", "").split(".")[-1]
        return None
    return df.apply(extract_value, axis=1)


def first_true_column(df: DataFrame, cols: list[str]) -> Series:
    """
    Retorna uma Series com o nome da primeira coluna (entre as especificadas)
    que contém True em cada linha.

    :param df: DataFrame contendo as colunas booleanas.
    :param cols: Lista de nomes de colunas booleanas.
    :return: Series com o nome da primeira coluna True por linha, ou None.
    """

    def find_first_true(row):
        for col in cols:
            if row[col]:
                return col
        return None

    return df[cols].apply(find_first_true, axis=1)