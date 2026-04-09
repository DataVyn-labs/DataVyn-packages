# DataVyn

**Simple, consistent connectors to load data from any source into a Pandas DataFrame.**

Built by [DataVyn Labs](https://github.com/datavynlabs)

---

## Why DataVyn?

Every datasource has a different library, different syntax, different setup. DataVyn gives you one clean interface across all of them.

```python
# Without DataVyn — different library for every source
import psycopg2
conn = psycopg2.connect(host=..., port=..., dbname=..., user=..., password=...)
cursor = conn.cursor()
cursor.execute("SELECT * FROM table")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)

# With DataVyn — same pattern everywhere
from datavyn import PostgreSQL
df = PostgreSQL(host="...", database="...", user="...", password="...").load_table("table")
```

---

## Installation

```bash
# Install for a specific datasource
pip install datavyn[kaggle]
pip install datavyn[postgres]
pip install datavyn[mysql]
pip install datavyn[mongodb]
pip install datavyn[snowflake]
pip install datavyn[aws]
pip install datavyn[gcs]
pip install datavyn[bigquery]

# Install everything
pip install datavyn[all]
```

---

## Supported Connectors

| Connector | Install | Import |
|---|---|---|
| Kaggle | `pip install datavyn[kaggle]` | `from datavyn import Kaggle` |
| PostgreSQL | `pip install datavyn[postgres]` | `from datavyn import PostgreSQL` |
| MySQL | `pip install datavyn[mysql]` | `from datavyn import MySQL` |
| MongoDB | `pip install datavyn[mongodb]` | `from datavyn import MongoDB` |
| Snowflake | `pip install datavyn[snowflake]` | `from datavyn import Snowflake` |
| AWS S3 | `pip install datavyn[aws]` | `from datavyn import AWSS3` |
| Google Cloud Storage | `pip install datavyn[gcs]` | `from datavyn import GCS` |
| Google BigQuery | `pip install datavyn[bigquery]` | `from datavyn import BigQuery` |

---

## Usage

### Kaggle

```python
from datavyn import Kaggle

kg = Kaggle(url="https://www.kaggle.com/datasets/username/dataset-name")

# Load full dataset
df = kg.load()

# Load only N rows (great for large datasets)
df = kg.load(sample=5000)

# Load in chunks
for chunk in kg.load(chunksize=10000):
    process(chunk)

# Load specific columns only
df = kg.load(columns=["age", "salary"])

# Check dataset info
kg.info()

# Clear local cache
kg.clear_cache()
```

> **Note:** Kaggle credentials are required. Place your `kaggle.json` at `~/.kaggle/kaggle.json`.
> Download it from [kaggle.com](https://www.kaggle.com) → Account → API → Create New Token.

---

### PostgreSQL

```python
from datavyn import PostgreSQL

pg = PostgreSQL(
    host="localhost",
    database="my_db",
    user="my_user",
    password="my_password",
    port=5432          # optional, default: 5432
)

# Run SQL query
df = pg.query("SELECT * FROM orders WHERE status = 'active'")

# Load full table
df = pg.load_table("customers")

# Load specific columns
df = pg.load_table("customers", columns=["id", "name", "email"])

# Sample large tables
df = pg.load_table("customers", sample=5000)

# Load in chunks
for chunk in pg.load_table("orders", chunksize=10000):
    process(chunk)

# List all tables
pg.list_tables()

# Auto-close connection
with PostgreSQL(host="...", database="...", user="...", password="...") as pg:
    df = pg.load_table("sales")
```

---

### MySQL

```python
from datavyn import MySQL

db = MySQL(
    host="localhost",
    database="my_db",
    user="my_user",
    password="my_password",
    port=3306          # optional, default: 3306
)

# Run SQL query
df = db.query("SELECT * FROM orders WHERE status = 'active'")

# Load full table
df = db.load_table("customers")

# Load specific columns
df = db.load_table("customers", columns=["id", "name", "email"])

# Sample large tables
df = db.load_table("customers", sample=5000)

# Load in chunks
for chunk in db.load_table("orders", chunksize=10000):
    process(chunk)

# Auto-close connection
with MySQL(host="...", database="...", user="...", password="...") as db:
    df = db.load_table("products")
```

---

### MongoDB

```python
from datavyn import MongoDB

# Local MongoDB
mg = MongoDB(host="localhost", port=27017, database="my_db")

# Local with authentication
mg = MongoDB(host="localhost", database="my_db", username="user", password="pass")

# MongoDB Atlas (cloud)
mg = MongoDB(
    database="my_db",
    uri="mongodb+srv://user:password@cluster.mongodb.net/my_db"
)

# Load full collection
df = mg.load_collection("users")

# Load specific fields
df = mg.load_collection("users", columns=["name", "email", "age"])

# Query with filters
df = mg.query("users", filters={"age": {"$gt": 25}})
df = mg.query("orders", filters={"status": "active"}, limit=1000)

# Sample large collections
df = mg.load_collection("users", sample=5000)

# Auto-close connection
with MongoDB(database="my_db", uri="mongodb+srv://...") as mg:
    df = mg.load_collection("products")
```

---

### Snowflake

```python
from datavyn import Snowflake

sf = Snowflake(
    account="abc123.us-east-1",
    user="your_user",
    password="your_password",
    warehouse="COMPUTE_WH",
    database="MY_DB",
    schema="PUBLIC",   # optional, default: PUBLIC
    role="MY_ROLE"     # optional
)

# Run SQL query
df = sf.query("SELECT * FROM sales WHERE year = 2024")

# Load full table
df = sf.load_table("orders")

# Load specific columns
df = sf.load_table("orders", columns=["id", "amount", "date"])

# Sample large tables
df = sf.load_table("orders", sample=5000)

# Load in chunks
for chunk in sf.load_table("orders", chunksize=10000):
    process(chunk)

# List tables and databases
sf.list_tables()
sf.list_databases()

# Auto-close connection
with Snowflake(account="...", user="...", password="...", warehouse="...", database="...") as sf:
    df = sf.query("SELECT * FROM users")
```

---

### AWS S3

```python
from datavyn import AWSS3

s3 = AWSS3(
    aws_access_key_id="your_access_key",
    aws_secret_access_key="your_secret_key",
    region_name="us-east-1"    # optional, default: us-east-1
)

# Read files directly into DataFrame (supports CSV, JSON, Parquet)
df = s3.read("my-bucket", "data/sales.csv")
df = s3.read("my-bucket", "data/users.json")
df = s3.read("my-bucket", "data/orders.parquet")

# Load specific columns
df = s3.read("my-bucket", "data/sales.csv", columns=["id", "amount"])

# Sample large files
df = s3.read("my-bucket", "data/sales.csv", sample=5000)

# List all files in a bucket
files = s3.list_files("my-bucket")
files = s3.list_files("my-bucket", prefix="data/2024/")

# Upload DataFrame to S3
s3.upload(df, "my-bucket", "output/result.csv")
s3.upload(df, "my-bucket", "output/result.parquet")
```

---

### Google Cloud Storage

```python
from datavyn import GCS

gcs = GCS(
    project_id="my-gcp-project",
    credentials_path="path/to/service_account.json"
)

# Read files directly into DataFrame (supports CSV, JSON, Parquet)
df = gcs.read("my-bucket", "data/sales.csv")
df = gcs.read("my-bucket", "data/users.json")
df = gcs.read("my-bucket", "data/orders.parquet")

# Load specific columns
df = gcs.read("my-bucket", "data/sales.csv", columns=["id", "amount"])

# Sample large files
df = gcs.read("my-bucket", "data/sales.csv", sample=5000)

# List all files in a bucket
files = gcs.list_files("my-bucket")
files = gcs.list_files("my-bucket", prefix="data/2024/")

# Upload DataFrame to GCS
gcs.upload(df, "my-bucket", "output/result.csv")
gcs.upload(df, "my-bucket", "output/result.parquet")
```

> **Note:** GCS credentials require a service account JSON key file.
> Download it from GCP Console → IAM → Service Accounts → Create Key.

---

### Google BigQuery

```python
from datavyn import BigQuery

bq = BigQuery(
    project_id="my-gcp-project",
    credentials_path="path/to/service_account.json"
)

# Run SQL query
df = bq.query("SELECT * FROM `my_project.my_dataset.my_table`")

# Load full table
df = bq.load_table("my_dataset", "my_table")

# Load specific columns
df = bq.load_table("my_dataset", "my_table", columns=["id", "revenue"])

# Sample large tables
df = bq.load_table("my_dataset", "my_table", sample=5000)

# Load in chunks
for chunk in bq.load_table("my_dataset", "my_table", chunksize=10000):
    process(chunk)

# List datasets and tables
bq.list_datasets()
bq.list_tables("my_dataset")
```

> **Note:** BigQuery credentials require a service account JSON key file.
> Download it from GCP Console → IAM → Service Accounts → Create Key.

---

## Handling Large Datasets

Every connector in DataVyn supports three ways to handle large data:

```python
# 1. Sample — load only N random rows
df = connector.load_table("big_table", sample=5000)

# 2. Chunks — process data piece by piece
for chunk in connector.load_table("big_table", chunksize=10000):
    process(chunk)

# 3. Columns — load only the columns you need
df = connector.load_table("big_table", columns=["id", "name", "amount"])
```

---

## License

MIT License — free to use in personal and commercial projects.

---

## Contributing

Pull requests are welcome! If you'd like to add a new connector or improve an existing one, feel free to open an issue or submit a PR on [GitHub](https://github.com/DataVyn-labs/DataVyn-packages).

---

Built with ❤️ by [DataVyn Labs](https://github.com/datavynlabs)