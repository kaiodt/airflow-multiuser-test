from airflow.providers.jdbc.hooks.jdbc import JdbcHook


class DB2Hook(JdbcHook):
    """
    Hook customizado para IBM DB2 que usa JDBC (jaydebeapi) com o driver DB2.
    """

    conn_name_attr = 'db2_conn_id'
    default_driver_class = 'com.ibm.db2.jcc.DB2Driver'
    default_driver_path = '/usr/local/airflow/jars/db2jcc4.jar'

    def __init__(
        self,
        db2_conn_id: str,
        driver_class: str | None = None,
        driver_path: str | None = None,
        *args,
        **kwargs,
    ):
        driver_class = driver_class or self.default_driver_class
        driver_path = driver_path or self.default_driver_path

        super().__init__(
            jdbc_conn_id=db2_conn_id,
            driver_class=driver_class,
            driver_path=driver_path,
            *args,
            **kwargs,
        )

        self.db2_conn_id = db2_conn_id
