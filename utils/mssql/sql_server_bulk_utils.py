"""
Módulo utilitário para leitura e escrita em massa no SQL Server.
"""

import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine


def fetch_chunk_with_rownum(
        engine: Engine,
        table: str,
        start: int,
        end: int,
        dtypes: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Lê um intervalo de linhas de uma tabela usando ROW_NUMBER para paginação.
    """
    query = f"""
        WITH cte AS (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
            FROM {table}
        )
        SELECT *
        FROM cte
        WHERE rn BETWEEN {start} AND {end}
        ORDER BY rn
    """

    df = pd.read_sql_query(
        sql=query,
        con=engine,
        dtype=dtypes,
    )

    # Remove a coluna auxiliar usada para paginação
    df = df.drop(columns=['rn'])

    return df


def insert_with_executemany(
        engine: Engine,
        df: pd.DataFrame,
        table: str,
        batch_size: int = 10000,
) -> int:
    """
    Insere um DataFrame em uma tabela do SQL Server usando cursor.executemany
    com fast_executemany habilitado.
    """
    if df.empty:
        return 0

    cols = list(df.columns)

    placeholders = ', '.join('?' for _ in cols)
    col_list = ', '.join(f'[{c}]' for c in cols)

    insert_query = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
    """

    def _convert_value(v):
        if v is pd.NA or pd.isna(v):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.datetime64,)):
            return pd.to_datetime(v).to_pydatetime()
        return v

    rows = [
        tuple(_convert_value(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    conn = engine.raw_connection()

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        total = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(insert_query, batch)
            total += len(batch)

        conn.commit()

    finally:
        cursor.close()
        conn.close()

    return total
