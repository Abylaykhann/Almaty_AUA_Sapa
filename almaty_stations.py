import requests
import json
from config import token, latlng


#url_stations = f'https://api.waqi.info/map/bounds?token={token}&latlng={latlng}&networks=all'
station_list = []

def get_stations():
    '''тянет с API-шки, все станции мониторинга качество воздуха по заданным координатам'''
    response = requests.get(f'https://api.waqi.info/map/bounds?token={token}&latlng={latlng}&networks=all')
    if response.status_code == 200:
        data_j = response.json()
        for row in data_j['data']:
            station_list.append(row['uid'])
        print('Все прошло успешно, проверьте.')
        print(f'В списке лежат ID, {len(station_list)} станции')
    else:
        print(response.status_code)
        print("ERROR")

get_stations()
print(station_list)