import pandas as pd


class PostgreSQL:
    """
    PostgreSQL Connector for DataVyn Labs

    Usage:
        from datavyn import PostgreSQL

        pg = PostgreSQL(
            host="localhost",
            port=5432,
            database="my_db",
            user="my_user",
            password="my_password"
        )

        # Run SQL query
        df = pg.query("SELECT * FROM my_table LIMIT 100")

        # Load full table
        df = pg.load_table("my_table")

        # List all tables
        pg.list_tables()
    """

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
    ):
        """
        Args:
            host     : PostgreSQL server host (e.g. 'localhost' or '192.168.1.1')
            database : Database name
            user     : PostgreSQL username
            password : PostgreSQL password
            port     : PostgreSQL port (default: 5432)
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
        """Creates or returns an existing PostgreSQL connection."""
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "The 'psycopg2' package is required.\n"
                "Install it with: pip install psycopg2-binary"
            )

        if self._conn is None or self._conn.closed:
            print("[DataVyn] Connecting to PostgreSQL ...")
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
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
        Load a full PostgreSQL table as a DataFrame.

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

    def list_tables(self) -> pd.DataFrame:
        """Lists all tables in the current database."""
        print(f"[DataVyn] Fetching tables in database: {self.database} ...")
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        return self._run(sql)

    def close(self):
        """Closes the PostgreSQL connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            print("[DataVyn] Connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()