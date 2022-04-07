import requests
import logging

from requests.models import HTTPBasicAuth, Response

class Extraction():

    def __init__(self, api) -> None:
        self.api = api

    def extractionWithParams(**kwargs):

        endpoint = kwargs.get('endpoint')
        query = kwargs.get('query')
        try:
            response = requests.get(endpoint, params=query, max_redirects = 2) #allow_redirects=False, max_redirects = 2)
            results = response.json()
            response.raise_for_status()
        except requests.exceptions.HTTPError as e: ## Para problemas de cliente
            print(e)

        logging.info("Worked just Fine")
        print(results)
        return results

    def simpleExtraction(self, **kwargs):
        api = kwargs.get('api')
        response = requests.get(api)
        if(response.status_code == 200):
            print('Worked Just Fine')
        elif(response.status_code == 404):
            print("results not founded")
        print("Data do request:", response.headers['date'])
        return response.json()

    def extractionWithAuth(**kwargs):
        endpoint = kwargs.get('endpoint')
        username = kwargs.get('username')
        password = kwargs.get('password')
        try:
            response = requests.get(endpoint, auth=HTTPBasicAuth(username, password), timeout= 5)
            response.raise_for_status()
            return response.json()
        except requests.ConnectionError as e: 
            print(e)
        except requests.Timeout as to:
            print(to)
        except requests.HTTPError as ht:
            print(ht)
        except requests.exceptions.RequestException as er:
            print(er)


Extraction.simpleExtraction(None, api = "http://api.open-notify.org/astros.json")

Extraction.extractionWithParams(endpoint = "http://api.open-notify.org/astros.json", query = None)

#https://github.com/andresionek91/IngestaoMercadoBitcoin doc