from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


class SQLServerHook(MsSqlHook):
    """
    Hook customizado para SQL Server que usa pyodbc.
    É possível confingurar o driver ODBC via código ou parâmetro extra na
    conexão do Airflow. Padrão: "ODBC Driver 18 for SQL Server".
    """

    conn_name_attr = 'mssql_conn_id'
    default_driver = 'ODBC Driver 18 for SQL Server'

    def __init__(
        self,
        mssql_conn_id: str,
        driver: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(mssql_conn_id=mssql_conn_id, *args, **kwargs)
        self.driver = driver or self.default_driver

    def get_sqlalchemy_engine(
        self,
        trust_cert: bool = True,
        fast_executemany: bool = True,
    ):
        """
        Retorna um SQLAlchemy engine configurado para SQL Server.

        :param trust_cert: Se True, ignora validação do certificado TLS.
        :param fast_executemany: Se True, habilita bulk insert mais rápido.
        """
        connection = self.get_connection(self.mssql_conn_id)

        # Permite sobrescrever driver via extras da conexão
        extra = connection.extra_dejson
        driver = extra.get('driver', self.driver)

        connection_url = URL.create(
            'mssql+pyodbc',
            username=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            database=self.schema or connection.schema,
            query={
                'driver': driver,
                'TrustServerCertificate': 'yes' if trust_cert else 'no',
            },
        )

        return create_engine(
            connection_url,
            fast_executemany=fast_executemany,
        )
