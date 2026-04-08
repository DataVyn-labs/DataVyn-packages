import os
import pandas as pd
from dotenv import load_dotenv


class Snowflake:
    """
    Snowflake Connector for DataVyn Labs

    Usage:
        from datavyn import Snowflake

        sf = Snowflake(env_file=".env")

        # Run SQL query
        df = sf.query("SELECT * FROM my_table LIMIT 100")

        # Load full table
        df = sf.load_table("my_table")

        # List tables / databases
        sf.list_tables()
        sf.list_databases()

    .env file format:
        SNOWFLAKE_ACCOUNT=abc123.us-east-1
        SNOWFLAKE_USER=your_user
        SNOWFLAKE_PASSWORD=your_password
        SNOWFLAKE_WAREHOUSE=COMPUTE_WH
        SNOWFLAKE_DATABASE=MY_DB
        SNOWFLAKE_SCHEMA=PUBLIC        (optional, default: PUBLIC)
        SNOWFLAKE_ROLE=MY_ROLE         (optional)
    """

    def __init__(self, env_file: str = ".env"):
        """
        Args:
            env_file : Path to your .env file (default: '.env')
        """
        load_dotenv(env_file)

        self.account   = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user      = os.getenv("SNOWFLAKE_USER")
        self.password  = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database  = os.getenv("SNOWFLAKE_DATABASE")
        self.schema    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.role      = os.getenv("SNOWFLAKE_ROLE")

        self._validate_credentials()
        self._conn = None

    def _validate_credentials(self):
        missing = []
        for field in ["account", "user", "password", "warehouse", "database"]:
            if not getattr(self, field):
                missing.append(f"SNOWFLAKE_{field.upper()}")

        if missing:
            raise EnvironmentError(
                f"Missing keys in your .env file: {', '.join(missing)}\n"
                "Make sure your .env file contains:\n"
                "  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,\n"
                "  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE"
            )

    def _get_connection(self):
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "The 'snowflake-connector-python' package is required.\n"
                "Install it with: pip install snowflake-connector-python"
            )

        if self._conn is None or self._conn.is_closed():
            print("[DataVyn] Connecting to Snowflake ...")
            conn_params = {
                "account"  : self.account,
                "user"     : self.user,
                "password" : self.password,
                "warehouse": self.warehouse,
                "database" : self.database,
                "schema"   : self.schema,
            }
            if self.role:
                conn_params["role"] = self.role

            self._conn = snowflake.connector.connect(**conn_params)
            print("[DataVyn] Connected successfully!")

        return self._conn

    def _run(self, sql: str) -> pd.DataFrame:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(rows, columns=columns)

    def query(self, sql: str, sample: int = None, chunksize: int = None):
        """
        Run any SQL query and return results as a DataFrame.

        Args:
            sql       : SQL query string
            sample    : Return only N random rows from result
            chunksize : Return an iterator of DataFrames
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

    def load_table(self, table_name: str, columns: list = None, sample: int = None, chunksize: int = None):
        """
        Load a full Snowflake table as a DataFrame.

        Args:
            table_name : Name of the table to load
            columns    : List of columns to select (default: all)
            sample     : Return only N random rows
            chunksize  : Return chunk iterator
        """
        cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {table_name}"
        print(f"[DataVyn] Loading table: {table_name}")
        return self.query(sql, sample=sample, chunksize=chunksize)

    def list_tables(self) -> pd.DataFrame:
        """Lists all tables in the current database and schema."""
        print(f"[DataVyn] Fetching tables in {self.database}.{self.schema} ...")
        return self._run(f"SHOW TABLES IN SCHEMA {self.database}.{self.schema}")

    def list_databases(self) -> pd.DataFrame:
        """Lists all databases in the Snowflake account."""
        print("[DataVyn] Fetching all databases ...")
        return self._run("SHOW DATABASES")

    def close(self):
        """Closes the Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            print("[DataVyn] Connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()