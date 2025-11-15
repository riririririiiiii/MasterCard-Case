import pandas as pd
from sqlalchemy import create_engine

try:
    # Замените на свой путь
    df = pd.read_parquet("example_dataset.parquet")
    print("✅ Файл parquet загружен")

    # Обязательно: форматируем дату
    df['transaction_timestamp'] = pd.to_datetime(df['transaction_timestamp'])
    print("✅ Дата отформатирована")

    # Подключение к твоей базе nurayCase
    engine = create_engine("mysql+mysqlconnector://case:1234@localhost:3306/nurayCase")
    print("✅ Подключение к БД установлено")

    # Загружаем данные порциями по 2000 строк
    chunk_size = 2000
    df.to_sql(name='transactions', con=engine, if_exists='replace', index=False, chunksize=chunk_size)
    print(f"✅ Данные успешно загружены! (всего {len(df)} строк)")

    # Проверка: выводим первые строки из БД
    result_df = pd.read_sql("SELECT * FROM transactions LIMIT 5", con=engine)
    print("\n📊 Первые 5 строк из БД:")
    print(result_df)

    # Подсчет количества строк в БД
    count_query = pd.read_sql("SELECT COUNT(*) as total_rows FROM transactions", con=engine)
    print(f"\n📈 Всего строк в таблице: {count_query['total_rows'][0]}")

except Exception as e:
    print(f"❌ Ошибка: {e}")
finally:
    print("\n✨ Скрипт завершен")