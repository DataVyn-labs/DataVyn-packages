import io
import pandas as pd


class AWSS3:
    """
    AWS S3 Connector for DataVyn Labs

    Usage:
        from datavyn import AWSS3

        s3 = AWSS3(
            aws_access_key_id="your_access_key",
            aws_secret_access_key="your_secret_key",
            region_name="us-east-1"
        )

        # Read a CSV file from S3
        df = s3.read("my-bucket", "folder/data.csv")

        # Read a JSON file
        df = s3.read("my-bucket", "folder/data.json")

        # Read a Parquet file
        df = s3.read("my-bucket", "folder/data.parquet")

        # List all files in a bucket
        files = s3.list_files("my-bucket")

        # Upload DataFrame to S3
        s3.upload(df, "my-bucket", "folder/output.csv")
    """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = "us-east-1",
    ):
        """
        Args:
            aws_access_key_id     : AWS access key ID
            aws_secret_access_key : AWS secret access key
            region_name           : AWS region (default: us-east-1)
        """
        self.aws_access_key_id     = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name           = region_name
        self._client               = None

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_client(self):
        """Creates or returns an existing S3 client."""
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "The 'boto3' package is required.\n"
                "Install it with: pip install boto3"
            )

        if self._client is None:
            print("[DataVyn] Connecting to AWS S3 ...")
            self._client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
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
        key: str,
        sample: int = None,
        columns: list = None,
    ) -> pd.DataFrame:
        """
        Read a file from S3 and return as a DataFrame.

        Args:
            bucket  : S3 bucket name
            key     : File path inside the bucket (e.g. 'folder/data.csv')
            sample  : Return only N random rows
            columns : List of columns to load (default: all)

        Returns:
            pd.DataFrame
        """
        client = self._get_client()
        fmt = self._detect_format(key)

        print(f"[DataVyn] Reading s3://{bucket}/{key} ...")
        response = client.get_object(Bucket=bucket, Key=key)
        content  = response["Body"].read()

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
        List all files in an S3 bucket.

        Args:
            bucket : S3 bucket name
            prefix : Filter files by prefix/folder (e.g. 'data/2024/')

        Returns:
            pd.DataFrame with file names, sizes and last modified dates
        """
        client = self._get_client()
        print(f"[DataVyn] Listing files in bucket: {bucket} ...")

        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            print("[DataVyn] No files found.")
            return pd.DataFrame()

        files = [
            {
                "file"         : obj["Key"],
                "size_kb"      : round(obj["Size"] / 1024, 2),
                "last_modified": obj["LastModified"],
            }
            for obj in response["Contents"]
        ]

        df = pd.DataFrame(files)
        print(f"[DataVyn] Found {len(df)} file(s)")
        return df

    def upload(
        self,
        df: pd.DataFrame,
        bucket: str,
        key: str,
    ):
        """
        Upload a DataFrame as a file to S3.

        Args:
            df     : Pandas DataFrame to upload
            bucket : S3 bucket name
            key    : Target file path in bucket (e.g. 'folder/output.csv')
        """
        client = self._get_client()
        fmt = self._detect_format(key)

        print(f"[DataVyn] Uploading to s3://{bucket}/{key} ...")

        buffer = io.BytesIO()

        if fmt == "csv":
            df.to_csv(buffer, index=False)
        elif fmt == "json":
            df.to_json(buffer, orient="records")
        elif fmt == "parquet":
            df.to_parquet(buffer, index=False)

        buffer.seek(0)
        client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        print(f"[DataVyn] Successfully uploaded {len(df):,} rows to s3://{bucket}/{key}")