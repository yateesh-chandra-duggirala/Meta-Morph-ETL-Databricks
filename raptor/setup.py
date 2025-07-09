from setuptools import setup, find_packages

long_description = """
Data Raptor is a comparison utility which can be used to compare data between various sources like Netezza, Snowflake & Big Data Data Lake.

• Powerful, easy-to-use utility for data comparison  
• Compares data for selected tables in two databases  
• Stores data mismatches as delta tables for further analysis  
• Provides atomic level (row by row, column by column) data mismatch results  
"""

setup(
    name="raptor",
    version="1.2.10",
    packages=find_packages(),
    description="A Utility for Data Comparison",
    long_description=long_description,
    author="Yateesh Chandra",
    author_email="yateed1437@gmail.com",
    install_requires=[
        "google-cloud-storage",
        "psycopg2-binary",
        "google-auth",
        "google-auth-oauthlib",
        "google-auth-httplib2",
    ],
)
