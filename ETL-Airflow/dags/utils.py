import logging
import requests
from airflow.decorators import task

# Define a function that gets Data from API
@task(task_id="fetch_data_from_api")
def get_api_call():

    # Provide the URL of API
    url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    
    # Make a Call to GET the data from URL
    response = requests.get(url)
    logging.info("Fetched Response")
    
    # Store the Reponse in the form of JSON
    res = response.json()
    return res