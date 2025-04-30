# setup.py

from setuptools import setup, find_packages

setup(
    name="metamorph",
    version="0.2",
    packages=find_packages(),
    description="A simple greeting package",
    author="Yateesh Chandra",
    author_email="yateed1437@gmail.com",
    install_requires=[
        "google-cloud-storage",
        "fsspec",
        "gcsfs",
        "smart_open",
        "sqlalchemy",
        "cloud-sql-python-connector",
        "google-auth ",
        "google-auth-oauthlib",
        "google-auth-httplib2",
        "apache-airflow",
        "apache-airflow-providers-postgres",
        "apache-airflow-providers-apache-spark",
        "python-jose[cryptography]"
    ],
)
