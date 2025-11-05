"""
Módulo utilitário para tratamento de dados originados de bancos de dados
SQL Server utilizando DataFrames Pandas.
"""

import pandas as pd
from sqlalchemy.engine import Engine


# =============================================================================
# Mapeamento de Tipos SQL Server → Pandas
# =============================================================================

SQL_TO_PANDAS = {
    # Numéricos
    'tinyint': 'Int64',
    'smallint': 'Int64',
    'int': 'Int64',
    'bigint': 'Int64',
    'decimal': 'float64',
    'numeric': 'float64',
    'float': 'float64',
    'real': 'float64',
    'money': 'float64',
    'smallmoney': 'float64',

    # Booleanos
    'bit': 'boolean',

    # Texto
    'char': 'string',
    'nchar': 'string',
    'varchar': 'string',
    'nvarchar': 'string',
    'text': 'string',
    'ntext': 'string',

    # Datas
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'datetime2': 'datetime64[ns]',
    'smalldatetime': 'datetime64[ns]',
    'time': 'string',  # manter como string se não precisar de timedelta
}


# =============================================================================
# Funções Utilitárias
# =============================================================================

def get_table_dtypes(
        engine: Engine,
        table: str,
        schema: str = 'dbo',
) -> dict[str, str]:
    """
    Obtém os tipos de dados esperados para uma tabela SQL Server
    e converte para pandas dtypes.
    """
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
    """

    df = pd.read_sql(query, con=engine)

    return {
        row['COLUMN_NAME']: SQL_TO_PANDAS.get(
            row['DATA_TYPE'].lower(), 'string'
        )
        for _, row in df.iterrows()
    }


def enforce_dtypes(df: pd.DataFrame, dtypes: dict[str, str]) -> pd.DataFrame:
    """
    Força os tipos de dados de um DataFrame conforme mapeamento esperado.
    Se a conversão falhar, usa string como fallback.
    """
    for col, dtype in dtypes.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                df[col] = df[col].astype('string')

    return df


def normalizar_strings(
        df: pd.DataFrame,
        string_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Remove espaços extras e converte strings vazias para NULL.
    """
    if string_cols is None:
        string_cols = [
            c for c in df.columns if pd.api.types.is_string_dtype(df[c])
        ]

    for col in string_cols:
        df[col] = (
            df[col]
            .astype('string')
            .str.strip()
            .replace('', pd.NA)
        )

    return df
