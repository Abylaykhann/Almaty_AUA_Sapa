import json
import requests
from config import token, station_ids

info_list = []

station_id = 12774


def get_info_from_all_stations():
    for station_id in station_ids:
        response = requests.get(f'https://api.waqi.info/feed/@{station_id}/?token={token}')

    # '''Это функция тянет все данные качества воздуха из станции типо в реальном времени'''
    # with open('all_station_result.json', 'w+', encoding='utf-8') as file:
    #     for station_id in stations_id:
    #         response = requests.get(f'https://api.waqi.info/feed/@{station_id}/?token={token}')
    #
    #         if response.status_code == 200:
    #             data = response.json()
    #             json.dump(data, file, indent=4)
    #             file.write('\n')
    #             print(f"Данные станции по ID№ {station_id}, сохранены")
    #         else:
    #             print(f"⚠️ Ошибка запроса для станции с ID№ {station_id}")

response = requests.get(f'https://api.waqi.info/feed/@{station_id}/?token={token}')
data = response.json()


with open('result.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)


print(data)