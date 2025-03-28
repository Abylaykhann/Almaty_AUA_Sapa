import requests
import pandas
from config import token, station_ids
from db_connector import save_to_postgresql
from datetime import datetime




def get_stations_data():
    """Функция получает данные со станций и сохраняет в один JSON-файл."""

    all_stations_data = []  # Список для хранения всех данных

    for station_id in station_ids:
        try:
            # Отправляем запрос к API
            response = requests.get(f'https://api.waqi.info/feed/@{station_id}/?token={token}')

            # Проверяем успешность запроса
            if response.status_code == 200:
                data = response.json()['data']

                # Парсим данные
                station_name = data['city'].get('name', None)
                air_quality_index = data.get('aqi', None)
                dominant_pollutant = data.get('dominentpol', None)
                pm10 = data.get('iaqi', {}).get('pm10', {}).get('v', None)
                pm25 = data.get('iaqi', {}).get('pm25', {}).get('v', None)
                nitrogen_dioxide = data.get('iaqi', {}).get('no2', {}).get('v', None)
                humidity = data.get('iaqi', {}).get('h', {}).get('v', None)
                #temperature = data.get('iaqi', {}).get('t', {}).get('v', None)
                #timestamp = data['time']['iso']
                location = data['city'].get('location', None)
                #latitude = data["city"]["geo"][0] if "city" in data else None
                #longitude = data["city"]["geo"][1] if "city" in data else None
                # Проверяем наличие ключа и преобразуем в datetime
                timestamp = data.get("time", {}).get("iso", None)
                if timestamp:
                    timestamp = timestamp.replace("Z", "")  # Убираем 'Z', если есть
                    try:
                        timestamp = datetime.fromisoformat(timestamp)  # Пробуем обычный формат
                    except ValueError:
                        timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")  # Если есть временная зона

                # Преобразуем координаты в строку (чтобы PostgreSQL принял их как POINT)

                coordinates = data.get('city', {}).get('geo', [None, None])
                coordinates_str = f"({coordinates[0]}, {coordinates[1]})"

                # Сохраняет в словарь
                station_info = {
                    'station_name': station_name,
                    'air_quality_index': air_quality_index,
                    'dominant_pollutant': dominant_pollutant,
                    'pm10': pm10,
                    'pm25': pm25,
                    'nitrogen_dioxide': nitrogen_dioxide,
                    'humidity': humidity,
                    #'температура': temperature,
                    'time': timestamp,
                    'location': location,
                    #"широта": latitude,
                    #"долгота": longitude,
                    'coordinates': coordinates_str,
                }

                all_stations_data.append(station_info)
                print(f"✅ Данные получены для станции {station_id}, подсчет - {len(all_stations_data)}")

            else:
                print(f"❌ Ошибка API (код {response.status_code}) для станции {station_id}")

        except Exception as ex:
            print(f"❌ Ошибка при обработке станции {station_id}: {ex}")

    # Записываем все данные в один JSON-файл
    # with open('all_station_result.json', "w", encoding="utf-8") as file:
    #     json.dump(all_data, file, indent=4, ensure_ascii=False)
    #df_aqi.to_csv("air_quality_data.csv", encoding="utf-8")
    df_aqi = pandas.DataFrame(all_stations_data)
    df_aqi.to_csv("air_quality_data.csv", index=False, encoding="utf-8")

    print("\n📁 Данные сохранены в CSV: air_quality_data.csv")

    return all_stations_data


# 🔹 Основной запуск
if __name__ == "__main__":
    station_data = get_stations_data()

    # Если данные есть, сохраняем в БД
    if station_data:
        save_to_postgresql(station_data)