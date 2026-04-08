# all imports 
import os
import zipfile
import pandas as pd
from pathlib import Path

# Kaggle connector.
class Kaggle:
    """
    Kaggle Dataset Connector for DataVyn Labs
    
    Usage:
        from datavyn import Kaggle
        
        df = Kaggle(url="https://www.kaggle.com/datasets/username/dataset-name").load()
        df = Kaggle(url="...").load(sample=5000)
        df = Kaggle(url="...").load(chunksize=10000)
    """

    def __init__(self, url: str, api_key: str = None, username: str = None):
        """
        Args:
            url      : Full Kaggle dataset URL
            api_key  : Kaggle API key (optional, reads from ~/.kaggle/kaggle.json if not provided)
            username : Kaggle username (optional, reads from ~/.kaggle/kaggle.json if not provided)
        """
        self.url = url
        self.slug = self._parse_slug(url)
        self.download_path = Path(".datavyn_cache") / self.slug.replace("/", "_")

        # Set credentials if provided directly
        if api_key and username:
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = api_key

        self._validate_credentials()

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _parse_slug(self, url: str) -> str:
        """Extracts 'username/dataset-name' from full Kaggle URL."""
        url = url.strip().rstrip("/")
        if "kaggle.com/datasets/" not in url:
            raise ValueError(
                f"Invalid Kaggle URL: '{url}'\n"
                "Expected format: https://www.kaggle.com/datasets/username/dataset-name"
            )
        slug = url.split("kaggle.com/datasets/")[-1]
        parts = slug.split("/")
        if len(parts) < 2:
            raise ValueError(
                f"Could not parse dataset slug from URL: '{url}'"
            )
        return f"{parts[0]}/{parts[1]}"

    def _validate_credentials(self):
        """Checks that Kaggle credentials are available."""
        kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
        has_env = os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")

        if not has_env and not kaggle_json.exists():
            raise EnvironmentError(
                "Kaggle credentials not found.\n"
                "Either:\n"
                "  1. Place your kaggle.json at ~/.kaggle/kaggle.json\n"
                "  2. Pass api_key and username to Kaggle()"
            )

    def _download(self):
        """Downloads and unzips the dataset into .datavyn_cache/"""
        try:
            import kaggle  # noqa: F401 — triggers kaggle auth check
        except ImportError:
            raise ImportError(
                "The 'kaggle' package is required. Install it with: pip install kaggle"
            )

        self.download_path.mkdir(parents=True, exist_ok=True)

        print(f"[DataVyn] Downloading dataset: {self.slug} ...")
        os.system(
            f"kaggle datasets download -d {self.slug} -p {self.download_path} --unzip"
        )
        print(f"[DataVyn] Download complete. Files saved to: {self.download_path}")

    def _get_csv_files(self) -> list:
        """Returns all CSV files in the download directory."""
        csv_files = list(self.download_path.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in dataset '{self.slug}'.\n"
                f"Download path: {self.download_path}"
            )
        return csv_files

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def load(
        self,
        sample: int = None,
        chunksize: int = None,
        columns: list = None,
        file_index: int = 0,
    ) -> pd.DataFrame:
        """
        Downloads and loads the Kaggle dataset as a Pandas DataFrame.

        Args:
            sample     : Load only N random rows (useful for large datasets)
            chunksize  : Load in chunks — returns a TextFileReader iterator
            columns    : List of column names to load (reduces memory usage)
            file_index : If dataset has multiple CSVs, pick which one (default: 0)

        Returns:
            pd.DataFrame or TextFileReader (if chunksize is set)
        """
        # Download only if not already cached
        if not self.download_path.exists() or not any(self.download_path.iterdir()):
            self._download()
        else:
            print(f"[DataVyn] Using cached dataset at: {self.download_path}")

        csv_files = self._get_csv_files()

        # Warn if multiple files exist
        if len(csv_files) > 1:
            print(
                f"[DataVyn] Multiple CSV files found: {[f.name for f in csv_files]}\n"
                f"[DataVyn] Loading file: '{csv_files[file_index].name}' "
                f"(use file_index=N to pick another)"
            )

        target_file = csv_files[file_index]
        print(f"[DataVyn] Loading: {target_file.name}")

        # --- Chunked loading (returns iterator) ---
        if chunksize:
            print(f"[DataVyn] Returning chunk iterator (chunksize={chunksize})")
            return pd.read_csv(target_file, chunksize=chunksize, usecols=columns)

        # --- Sampled loading ---
        if sample:
            print(f"[DataVyn] Sampling {sample} rows ...")
            # Count total rows first (efficient)
            total_rows = sum(1 for _ in open(target_file)) - 1  # subtract header
            if sample >= total_rows:
                print(f"[DataVyn] Sample size ({sample}) >= total rows ({total_rows}). Loading full dataset.")
                return pd.read_csv(target_file, usecols=columns)
            return pd.read_csv(target_file, usecols=columns).sample(n=sample, random_state=42)

        # --- Full load ---
        df = pd.read_csv(target_file, usecols=columns)
        print(f"[DataVyn] Loaded {len(df):,} rows x {len(df.columns)} columns")
        return df

    def info(self):
        """Prints dataset slug and cached file info without loading into memory."""
        print(f"Dataset : {self.slug}")
        print(f"URL     : {self.url}")
        if self.download_path.exists():
            csv_files = self._get_csv_files()
            for i, f in enumerate(csv_files):
                size_mb = f.stat().st_size / (1024 * 1024)
                print(f"File [{i}]: {f.name}  ({size_mb:.2f} MB)")
        else:
            print("Status  : Not yet downloaded")

    def clear_cache(self):
        """Deletes the locally cached dataset files."""
        import shutil
        if self.download_path.exists():
            shutil.rmtree(self.download_path)
            print(f"[DataVyn] Cache cleared: {self.download_path}")
        else:
            print("[DataVyn] No cache found to clear.")