from concurrent.futures import Executor, ThreadPoolExecutor
import re
from tokenize import String
import requests
import json
import logging
import time

class Pokemon():

    def __init__(self) -> None:
        self.url = "https://pokeapi.co/api/v2/"

    def _get_url(self) -> String:
        return self.url + "pokemon/"

    def get_pokemon(self):
        url = self.url + "pokemon/"
        response = requests.get(url)
        return response.json()
    
    def get_total(self):
        #Getting total amount of pokemons - API DOCUMENTATION
        request = self.get_pokemon()
        return json.loads(json.dumps(request)).get('count')

    def load_all_pokemons(self):
        url = self._get_url()
        total_data = []
        response = requests.get(url)
        i = 0
        while response.status_code == 200:
            i+=1
            try:
                #print(f"requesting {i}")
                response = requests.get(url + f"{i}/")
                response.raise_for_status()
                total_data.append(json.dumps(response.json()) + "\n")
            except requests.exceptions.HTTPError as e: 
                print(f"error: {e}")
            #if response.status_code != 204 and response.headers["content-type"].strip().startswith('application/json'):

        return total_data
            
    def _write_file(self):
        timer = time.time()
        data = self.load_all_pokemons()
        print("Saving file..")
        with open ('/Users/zeuser/Desktop/Scripts/Python/full_ingestion_two.json', 'w') as f:
            f.writelines(data)            
            #f.write("\n".join(map(json.dumps, data)))
            #for item in data:
            #    f.write(item) Storing data with loop
        
        print("Total time:", time.time() - timer)
    
Pokemon()._write_file()