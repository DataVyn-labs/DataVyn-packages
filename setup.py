from setuptools import setup, find_packages

setup(
    name="datavyn",
    version="0.1.0",
    author="DataVyn Labs",
    author_email="contact@datavynlabs.com",
    description="Simple connectors to load data from Kaggle, databases, cloud, and more.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/datavynlabs/datavyn",
    packages=find_packages(),
    install_requires=[
        "pandas",                          # core — used by all connectors
    ],
    extras_require={
        "kaggle"    : ["kaggle"],                                # Kaggle connector
        "snowflake" : ["snowflake-connector-python"],            # Snowflake connector
        "postgres"  : ["psycopg2-binary"],                       # PostgreSQL connector
        "mongodb"   : ["pymongo"],                               # MongoDB connector
        "mysql"     : ["mysql-connector-python"],                # MySQL connector
        "aws"       : ["boto3"],                                 # AWS S3 connector
        "gcs"       : ["google-cloud-storage"],                  # Google Cloud Storage
        "bigquery"  : ["google-cloud-bigquery"],                 # Google BigQuery
        "all"       : [                                          # Install everything
            "kaggle",
            "snowflake-connector-python",
            "psycopg2-binary",
            "pymongo",
            "mysql-connector-python",
            "boto3",
            "google-cloud-storage",
            "google-cloud-bigquery",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    keywords="datavyn kaggle snowflake postgresql mongodb mysql aws s3 bigquery gcs connector dataframe",
)
