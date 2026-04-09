import pandas as pd


class MySQL:
    """
    MySQL Connector for DataVyn Labs

    Usage:
        from datavyn import MySQL

        db = MySQL(
            host="localhost",
            port=3306,
            database="my_db",
            user="my_user",
            password="my_password"
        )

        # Run SQL query
        df = db.query("SELECT * FROM my_table LIMIT 100")

        # Load full table
        df = db.load_table("my_table")
    """

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 3306,
    ):
        """
        Args:
            host     : MySQL server host (e.g. 'localhost' or '192.168.1.1')
            database : Database name
            user     : MySQL username
            password : MySQL password
            port     : MySQL port (default: 3306)
        """
        self.host     = host
        self.database = database
        self.user     = user
        self.password = password
        self.port     = port
        self._conn    = None

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_connection(self):
        """Creates or returns an existing MySQL connection."""
        try:
            import mysql.connector
        except ImportError:
            raise ImportError(
                "The 'mysql-connector-python' package is required.\n"
                "Install it with: pip install mysql-connector-python"
            )

        if self._conn is None or not self._conn.is_connected():
            print("[DataVyn] Connecting to MySQL ...")
            self._conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            print("[DataVyn] Connected successfully!")

        return self._conn

    def _run(self, sql: str) -> pd.DataFrame:
        """Internal: executes SQL and returns a DataFrame."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(rows, columns=columns)

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def query(self, sql: str, sample: int = None, chunksize: int = None):
        """
        Run any SQL query and return results as a DataFrame.

        Args:
            sql       : SQL query string
            sample    : Return only N random rows from result
            chunksize : Return an iterator of DataFrames

        Returns:
            pd.DataFrame or iterator
        """
        print("[DataVyn] Running query ...")
        df = self._run(sql)
        print(f"[DataVyn] Query returned {len(df):,} rows x {len(df.columns)} columns")

        if sample and sample < len(df):
            print(f"[DataVyn] Sampling {sample} rows ...")
            df = df.sample(n=sample, random_state=42)

        if chunksize:
            print(f"[DataVyn] Returning chunk iterator (chunksize={chunksize})")
            return (df[i:i + chunksize] for i in range(0, len(df), chunksize))

        return df

    def load_table(
        self,
        table_name: str,
        columns: list = None,
        sample: int = None,
        chunksize: int = None,
    ):
        """
        Load a full MySQL table as a DataFrame.

        Args:
            table_name : Name of the table to load
            columns    : List of columns to select (default: all)
            sample     : Return only N random rows
            chunksize  : Return chunk iterator

        Returns:
            pd.DataFrame or iterator
        """
        cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {table_name}"
        print(f"[DataVyn] Loading table: {table_name}")
        return self.query(sql, sample=sample, chunksize=chunksize)

    def close(self):
        """Closes the MySQL connection."""
        if self._conn and self._conn.is_connected():
            self._conn.close()
            print("[DataVyn] Connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()