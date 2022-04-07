from datetime import date, datetime, timedelta
from pkgutil import get_data
import requests as requests
import json

class Period:
    def __init__(self, dat_init, days_forward) -> None:
        self.dat_init = dat_init
        self.days_forward = days_forward
           
    def extractPeriod(self):
        start = datetime.strptime(self.dat_init, '%Y-%m-%d')
        data = []
        for day in range(self.days_forward):
            date = (start + timedelta(days = day))
            data.append(date)
        return data

    def get_all_data(self):
        all_dates = self.period()
        data = []
        for i in all_dates:
            row = json.dumps(self.get_data(i))
            data.append(row)

        return data

class BitcoinAPI():

    def __init__(self, coin, start_date, days_forward) -> None:
        self.coin = coin
        self.start_date = start_date
        self.days_forward = days_forward
        self.base_endpoint = 'https://www.mercadobitcoin.net/api'

    def _get_endpoint(self, date: datetime.date):
        return f"{self.base_endpoint}/{self.coin}/day-summary/{date.year}/{date.month}/{date.day}"

    def get_data(self, date: datetime.date) -> dict:
        endpoint = self._get_endpoint(date = date)
        response = requests.get(endpoint)
        return response.json()

    def period(self):
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        data = []
        for day in range(self.days_forward):
            date = (start_date + timedelta(days = day))
            data.append(date)
        return data

    def get_name_file(self):
        return f"data-{self.coin}-{self.start_date}-{self.days_forward}.txt"

    def write_to_file(self):
        #period = kwargs.get('period')
        file = self.get_name_file()
        all_dates = self.period()
        for i in all_dates:
            data = self.get_data(i)
            with open(f'/Users/matheus.sivelli/Desktop/py/{file}', "a") as f:
                f.write(json.dumps(data) + "\n")

BitcoinAPI(coin='BTC', start_date = '2020-01-01', days_forward=30).write_to_file()