import pandas as pd


class BigQuery:
    """
    Google BigQuery Connector for DataVyn Labs

    Usage:
        from datavyn import BigQuery

        bq = BigQuery(
            project_id="my-gcp-project",
            credentials_path="path/to/service_account.json"
        )

        # Run SQL query
        df = bq.query("SELECT * FROM my_dataset.my_table LIMIT 100")

        # Load full table
        df = bq.load_table("my_dataset", "my_table")

        # List datasets
        bq.list_datasets()

        # List tables in a dataset
        bq.list_tables("my_dataset")
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: str,
    ):
        """
        Args:
            project_id       : Google Cloud project ID
            credentials_path : Path to your service account JSON key file
                               Download it from GCP Console → IAM → Service Accounts
        """
        self.project_id       = project_id
        self.credentials_path = credentials_path
        self._client          = None

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_client(self):
        """Creates or returns an existing BigQuery client."""
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "The 'google-cloud-bigquery' package is required.\n"
                "Install it with: pip install google-cloud-bigquery"
            )

        if self._client is None:
            print("[DataVyn] Connecting to Google BigQuery ...")
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            self._client = bigquery.Client(
                project=self.project_id,
                credentials=credentials,
            )
            print("[DataVyn] Connected successfully!")

        return self._client

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def query(
        self,
        sql: str,
        sample: int = None,
        chunksize: int = None,
    ):
        """
        Run any SQL query on BigQuery and return results as a DataFrame.

        Args:
            sql       : Standard SQL query string
            sample    : Return only N random rows from result
            chunksize : Return an iterator of DataFrames

        Returns:
            pd.DataFrame or iterator
        """
        client = self._get_client()
        print("[DataVyn] Running query ...")

        df = client.query(sql).to_dataframe()
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
        dataset: str,
        table_name: str,
        columns: list = None,
        sample: int = None,
        chunksize: int = None,
    ):
        """
        Load a full BigQuery table as a DataFrame.

        Args:
            dataset    : BigQuery dataset name
            table_name : BigQuery table name
            columns    : List of columns to select (default: all)
            sample     : Return only N random rows
            chunksize  : Return chunk iterator

        Returns:
            pd.DataFrame or iterator
        """
        cols = ", ".join(columns) if columns else "*"
        sql  = f"SELECT {cols} FROM `{self.project_id}.{dataset}.{table_name}`"
        print(f"[DataVyn] Loading table: {dataset}.{table_name}")
        return self.query(sql, sample=sample, chunksize=chunksize)

    def list_datasets(self) -> pd.DataFrame:
        """Lists all datasets in the project."""
        client = self._get_client()
        print(f"[DataVyn] Fetching datasets in project: {self.project_id} ...")

        datasets = list(client.list_datasets())

        if not datasets:
            print("[DataVyn] No datasets found.")
            return pd.DataFrame()

        df = pd.DataFrame([{"dataset": d.dataset_id} for d in datasets])
        print(f"[DataVyn] Found {len(df)} dataset(s)")
        return df

    def list_tables(self, dataset: str) -> pd.DataFrame:
        """
        Lists all tables in a BigQuery dataset.

        Args:
            dataset : BigQuery dataset name

        Returns:
            pd.DataFrame with table names
        """
        client = self._get_client()
        print(f"[DataVyn] Fetching tables in dataset: {dataset} ...")

        tables = list(client.list_tables(dataset))

        if not tables:
            print("[DataVyn] No tables found.")
            return pd.DataFrame()

        df = pd.DataFrame([{"table": t.table_id} for t in tables])
        print(f"[DataVyn] Found {len(df)} table(s)")
        return df