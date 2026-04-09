import pandas as pd


class MongoDB:
    """
    MongoDB Connector for DataVyn Labs

    Usage:
        from datavyn import MongoDB

        mg = MongoDB(
            host="localhost",
            port=27017,
            database="my_db"
        )

        # For MongoDB Atlas (cloud)
        mg = MongoDB(
            uri="mongodb+srv://user:password@cluster.mongodb.net/my_db"
        )

        # Run a query
        df = mg.query("my_collection", filters={"age": {"$gt": 25}})

        # Load full collection
        df = mg.load_collection("my_collection")
    """

    def __init__(
        self,
        database: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = None,
        password: str = None,
        uri: str = None,
    ):
        """
        Args:
            database : MongoDB database name
            host     : MongoDB host (default: localhost)
            port     : MongoDB port (default: 27017)
            username : MongoDB username (optional)
            password : MongoDB password (optional)
            uri      : Full MongoDB URI — use this for MongoDB Atlas
                       e.g. 'mongodb+srv://user:pass@cluster.mongodb.net/db'
        """
        self.database = database
        self.host     = host
        self.port     = port
        self.username = username
        self.password = password
        self.uri      = uri
        self._client  = None

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_client(self):
        """Creates or returns an existing MongoDB client."""
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError(
                "The 'pymongo' package is required.\n"
                "Install it with: pip install pymongo"
            )

        if self._client is None:
            print("[DataVyn] Connecting to MongoDB ...")

            if self.uri:
                # Atlas or full URI connection
                self._client = MongoClient(self.uri)
            elif self.username and self.password:
                # Local with auth
                self._client = MongoClient(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )
            else:
                # Local without auth
                self._client = MongoClient(
                    host=self.host,
                    port=self.port,
                )

            # Ping to verify connection
            self._client.admin.command("ping")
            print("[DataVyn] Connected successfully!")

        return self._client

    def _get_db(self):
        """Returns the target database."""
        return self._get_client()[self.database]

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def query(
        self,
        collection: str,
        filters: dict = None,
        columns: list = None,
        sample: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Query a MongoDB collection with optional filters.

        Args:
            collection : Name of the MongoDB collection
            filters    : MongoDB filter dict (e.g. {"age": {"$gt": 25}})
            columns    : List of fields to return (default: all)
            sample     : Return only N random rows
            limit      : Limit number of documents returned

        Returns:
            pd.DataFrame
        """
        db = self._get_db()
        print(f"[DataVyn] Querying collection: {collection} ...")

        filters = filters or {}

        # Build projection from columns list
        projection = None
        if columns:
            projection = {col: 1 for col in columns}
            projection["_id"] = 0  # exclude _id by default
        else:
            projection = {"_id": 0}

        cursor = db[collection].find(filters, projection)

        if limit:
            cursor = cursor.limit(limit)

        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("[DataVyn] No documents found matching the query.")
            return df

        print(f"[DataVyn] Query returned {len(df):,} rows x {len(df.columns)} columns")

        if sample and sample < len(df):
            print(f"[DataVyn] Sampling {sample} rows ...")
            df = df.sample(n=sample, random_state=42)

        return df

    def load_collection(
        self,
        collection: str,
        columns: list = None,
        sample: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Load an entire MongoDB collection as a DataFrame.

        Args:
            collection : Name of the MongoDB collection
            columns    : List of fields to return (default: all)
            sample     : Return only N random rows
            limit      : Limit number of documents returned

        Returns:
            pd.DataFrame
        """
        print(f"[DataVyn] Loading collection: {collection} ...")
        return self.query(
            collection,
            filters={},
            columns=columns,
            sample=sample,
            limit=limit,
        )

    def close(self):
        """Closes the MongoDB connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            print("[DataVyn] Connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()