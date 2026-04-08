#  connector of all files in this package
from .kaggle import Kaggle
from .snowflake import Snowflake

# package version & info
__version__ = "0.1.0"
__all__ = ["Kaggle", "Snowflake"]