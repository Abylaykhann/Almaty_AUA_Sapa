import psycopg2
from config import db_host, db_name, db_port, db_user, db_password

# функция для фильтрации значений так как выключенные станции присылают такие: (заменяет "-" и пустые строки на None)
def clean_value(value: any):
    if isinstance(value, str):
        value = value.strip()  # Убираем пробелы по краям
    return None if value in ["-", "", None] else value

# по скольку названия длинные, обрезаем. Это функция обрезает все что встречается далее третьей запятой
def location_cleaner(value):
    if not isinstance(value, str):
        return value
    parts = value.split(",")
    if len(parts) > 3:
        value = ",".join(parts[:3])
    return value.strip()

def save_to_postgresql(data):
    """
    Функция сохраняет данные в БД
    """

    if not data:
        print("⚠️ Нет данных для записи в базу.")
        return

    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            host = db_host,
            dbname = db_name,
            user = db_user,
            password = db_password,
            port = db_port
        )
        cursor = connection.cursor()

        # Создаем таблицу, если её нет
        create_table_query = """
        CREATE TABLE IF NOT EXISTS air_quality (
            id SERIAL PRIMARY KEY,
            station_name VARCHAR(128),
            air_quality_index INT,
            dominant_pollutant VARCHAR(20),
            pm10 FLOAT,
            pm25 FLOAT,
            nitrogen_dioxide FLOAT,
            humidity FLOAT,
            timestamp TIMESTAMP,
            location VARCHAR(256),
            coordinates POINT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO air_quality (
            station_name, 
            air_quality_index, 
            dominant_pollutant,
            pm10, 
            pm25, 
            nitrogen_dioxide, 
            humidity, 
            timestamp, 
            location, 
            coordinates) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, POINT(%s));
        """

        # Формируем данные для записи в БД
        records = []

        # Проходим по каждому словарю в data
        for el in data:
            location_cleaned = location_cleaner(el['location'])
            record = (
                clean_value(el['station_name']),
                clean_value(el['air_quality_index']),
                clean_value(el['dominant_pollutant']),
                clean_value(el['pm10']),
                clean_value(el['pm25']),
                clean_value(el['nitrogen_dioxide']),
                clean_value(el['humidity']),
                clean_value(el['time']),
                location_cleaned,
                clean_value(el['coordinates']))

            # Добавляем запись в список
            records.append(record)

        for record in records:
            for i, val in enumerate(record):
                if isinstance(val, str) and len(val) > 100:  # Проверка строк длиннее 100 символов
                    print(f"⚠️ Длинное значение: {val} (длина: {len(val)}) в поле {i}")

        # Выполняем массовую вставку
        cursor.executemany(insert_query, records)
        connection.commit()

        # Закрываем соединение
        cursor.close()
        connection.close()
        print("✅ Данные успешно добавлены в БД.")

    except Exception as ex:
        print(f"❌ Ошибка при сохранении в БД: {ex}")
