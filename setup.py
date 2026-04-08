from setuptools import setup, find_packages

setup(
    name="datavyn",
    version="0.1.0",
    author="DataVyn Labs",
    description="Simple connectors to load data from Kaggle, databases, cloud, and more.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "kaggle",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)