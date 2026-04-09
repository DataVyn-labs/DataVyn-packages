import io
import pandas as pd


class GCS:
    """
    Google Cloud Storage Connector for DataVyn Labs

    Usage:
        from datavyn import GCS

        gcs = GCS(
            project_id="my-gcp-project",
            credentials_path="path/to/service_account.json"
        )

        # Read a file from GCS
        df = gcs.read("my-bucket", "folder/data.csv")

        # List all files in a bucket
        files = gcs.list_files("my-bucket")

        # Upload DataFrame to GCS
        gcs.upload(df, "my-bucket", "folder/output.csv")
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
        """Creates or returns an existing GCS client."""
        try:
            from google.cloud import storage
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "The 'google-cloud-storage' package is required.\n"
                "Install it with: pip install google-cloud-storage"
            )

        if self._client is None:
            print("[DataVyn] Connecting to Google Cloud Storage ...")
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            self._client = storage.Client(
                project=self.project_id,
                credentials=credentials,
            )
            print("[DataVyn] Connected successfully!")

        return self._client

    def _detect_format(self, key: str) -> str:
        """Detects file format from the file extension."""
        if key.endswith(".csv"):
            return "csv"
        elif key.endswith(".json"):
            return "json"
        elif key.endswith(".parquet"):
            return "parquet"
        else:
            raise ValueError(
                f"Unsupported file format for: '{key}'\n"
                "Supported formats: .csv, .json, .parquet"
            )

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def read(
        self,
        bucket: str,
        blob_path: str,
        sample: int = None,
        columns: list = None,
    ) -> pd.DataFrame:
        """
        Read a file from GCS and return as a DataFrame.

        Args:
            bucket    : GCS bucket name
            blob_path : File path inside the bucket (e.g. 'folder/data.csv')
            sample    : Return only N random rows
            columns   : List of columns to load (default: all)

        Returns:
            pd.DataFrame
        """
        client = self._get_client()
        fmt = self._detect_format(blob_path)

        print(f"[DataVyn] Reading gs://{bucket}/{blob_path} ...")
        bucket_obj = client.bucket(bucket)
        blob       = bucket_obj.blob(blob_path)
        content    = blob.download_as_bytes()

        if fmt == "csv":
            df = pd.read_csv(io.BytesIO(content), usecols=columns)
        elif fmt == "json":
            df = pd.read_json(io.BytesIO(content))
            if columns:
                df = df[columns]
        elif fmt == "parquet":
            df = pd.read_parquet(io.BytesIO(content), columns=columns)

        print(f"[DataVyn] Loaded {len(df):,} rows x {len(df.columns)} columns")

        if sample and sample < len(df):
            print(f"[DataVyn] Sampling {sample} rows ...")
            df = df.sample(n=sample, random_state=42)

        return df

    def list_files(self, bucket: str, prefix: str = "") -> pd.DataFrame:
        """
        List all files in a GCS bucket.

        Args:
            bucket : GCS bucket name
            prefix : Filter files by prefix/folder (e.g. 'data/2024/')

        Returns:
            pd.DataFrame with file names, sizes and last updated dates
        """
        client = self._get_client()
        print(f"[DataVyn] Listing files in bucket: {bucket} ...")

        blobs = client.list_blobs(bucket, prefix=prefix)

        files = [
            {
                "file"        : blob.name,
                "size_kb"     : round(blob.size / 1024, 2),
                "last_updated": blob.updated,
            }
            for blob in blobs
        ]

        if not files:
            print("[DataVyn] No files found.")
            return pd.DataFrame()

        df = pd.DataFrame(files)
        print(f"[DataVyn] Found {len(df)} file(s)")
        return df

    def upload(
        self,
        df: pd.DataFrame,
        bucket: str,
        blob_path: str,
    ):
        """
        Upload a DataFrame as a file to GCS.

        Args:
            df        : Pandas DataFrame to upload
            bucket    : GCS bucket name
            blob_path : Target file path in bucket (e.g. 'folder/output.csv')
        """
        client = self._get_client()
        fmt = self._detect_format(blob_path)

        print(f"[DataVyn] Uploading to gs://{bucket}/{blob_path} ...")

        buffer = io.BytesIO()

        if fmt == "csv":
            df.to_csv(buffer, index=False)
        elif fmt == "json":
            df.to_json(buffer, orient="records")
        elif fmt == "parquet":
            df.to_parquet(buffer, index=False)

        buffer.seek(0)
        bucket_obj = client.bucket(bucket)
        blob       = bucket_obj.blob(blob_path)
        blob.upload_from_file(buffer, rewind=True)

        print(f"[DataVyn] Successfully uploaded {len(df):,} rows to gs://{bucket}/{blob_path}")