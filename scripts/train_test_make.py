import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split

# Настройки подключения к базе данных
DATABASE_URL = "postgresql://postgres:vjcrdf@localhost:5432/ml_project"

def prepare_dataset(engine):
    print("Извлечение P2P логов из PostgreSQL...")
    p2p_chunks = pd.read_sql_query("SELECT * FROM p2p_log", engine, chunksize=100000)
    p2p = pd.concat(p2p_chunks, ignore_index=True)
    
    # ИСПРАВЛЕНИЕ: Агрегируем 17+ миллионов строк средствами PostgreSQL.
    # Это разгружает сеть и RAM, ускоряя процесс в разы.
    print("Расчет признаков (Feature Engineering) на стороне PostgreSQL...")
    aggregated_trans_sql = """
        SELECT 
            userid,
            COUNT(amount)::INT as trans_count,
            SUM(amount)::REAL as trans_sum,
            AVG(amount)::REAL as trans_mean,
            MAX(amount)::REAL as trans_max,
            MIN(amount)::REAL as trans_min,
            STDDEV(amount)::REAL as trans_std
        FROM trans_log
        GROUP BY userid;
    """
    
    trans_chunks = pd.read_sql_query(aggregated_trans_sql, engine, chunksize=100000)
    trans_features = pd.concat(trans_chunks, ignore_index=True)
    
    # Заполняем пустоты в среднеквадратичном отклонении (если у юзера 1 транзакция)
    trans_features['trans_std'] = trans_features['trans_std'].fillna(0).astype('float32')
    
    print("Объединение признаков по UserID...")
    df = pd.merge(p2p, trans_features, on='userid', how='left')
    df = df.fillna(0)
    
    # Явное освобождение памяти
    del trans_features
    del p2p
    
    print("Парсинг временных меток и удаление нечисловых ID...")
    df['eventtime'] = pd.to_datetime(df['eventtime'])
    
    # Переводим временные признаки в легкий формат int8
    df['hour'] = df['eventtime'].dt.hour.astype('int8')
    df['day_of_week'] = df['eventtime'].dt.dayofweek.astype('int8')
    
    # Оптимизация типов данных оставшихся столбцов
    df['amount'] = df['amount'].astype('float32')
    if 'isfraud' in df.columns:
        # Убедимся, что таргет корректно преобразуется в бинарный int8
        df['isfraud'] = df['isfraud'].astype(bool).astype('int8')
    
    # Удаление нечисловых признаков
    cols_to_drop = ['id', 'userid', 'recipientid', 'eventtime']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # Кодирование категориального признака валюты через факторизацию
    if 'currency' in df.columns:
        df['currency'] = pd.factorize(df['currency'])[0]
        df['currency'] = df['currency'].astype('int8')
    
    df.columns = df.columns.str.lower()
    return df

def split_export_and_upload(df, engine):
    X = df.drop(columns=['isfraud'])
    y = df['isfraud']
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=0.2, 
        random_state=42, 
        stratify=y
    )
    
    train_df = X_train.copy()
    train_df['isfraud'] = y_train
    
    test_df = X_test.copy()
    test_df['isfraud'] = y_test
    
    # 1. ЗАПИСЬ РЕЗУЛЬТАТОВ В POSTGRESQL
    # ИСПРАВЛЕНИЕ ОШИБКИ МЕДЛЕННОЙ ЗАПИСИ: добавлен метод 'multi' и chunksize
    print("Запись обучающей выборки (train_dataset) в PostgreSQL...")
    train_df.to_sql('train_dataset', engine, if_exists='replace', index=False, method='multi', chunksize=10000)
    
    print("Запись тестовой выборки (test_dataset) в PostgreSQL...")
    test_df.to_sql('test_dataset', engine, if_exists='replace', index=False, method='multi', chunksize=10000)
    
    # 2. СОХРАНЕНИЕ В ЛОКАЛЬНЫЕ CSV-ФАЙЛЫ
    print("Сохранение файла train_dataset.csv на диск...")
    train_df.to_csv('train_dataset.csv', index=False)
    
    print("Сохранение файла test_dataset.csv на диск...")
    test_df.to_csv('test_dataset.csv', index=False)
    
    return train_df, test_df

if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    
    final_df = prepare_dataset(engine)
    train_df, test_df = split_export_and_upload(final_df, engine)
    
    print("\n[УСПЕХ] Оптимизированный пайплайн подготовки данных завершен!")
    print(f"Размер обучающей выборки (train): {train_df.shape}")
    print(f"Размер тестовой выборки (test):  {test_df.shape}")
    print(f"Процент фрод-транзакций в train:  {train_df['isfraud'].mean():.4%}")