from concurrent.futures import Executor, ThreadPoolExecutor
import re
from tokenize import String
import requests
import json
import logging
import time

class Pokemon():

    def __init__(self) -> None:
        self.url = 'https://pokeapi.co/api/v2/'
    
    def _get_url(self) -> String:
        return self.url + "pokemon/"

    def get_url_list(self):
        return [self._get_url() + str(i) for i in range (1, 899)] ##HOW TO GET BETTER IN THIS??
    
    def requesting(self, url, timeout = 10):
        response = requests.get(url, timeout = timeout)
        return response.json()

    def multiThreadProcessing(self):
        results = []
        print("Running..")
        Executor = ThreadPoolExecutor(max_workers = 50)
        for result in Executor.map(self.requesting, self.get_url_list(), timeout = 10):
            results.append(json.dumps(result) + "\n")

        return results

    def write(self):
        timer = time.time()
        data = self.multiThreadProcessing()
        print("Saving file..")
        with open ('/Users/zeuser/Desktop/Scripts/Python/full_ingestion_multi_thread.json', 'w') as f:
            f.writelines(data)
        
        print("total_time", time.time() - timer)

Pokemon().write()