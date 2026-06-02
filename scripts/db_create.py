import pandas as pd
from sqlalchemy import create_engine, text  

# Настройки подключения
DB_CONFIG = {
    "dbname": "ml_project",
    "user": "postgres",
    "password": "vjcrdf", 
    "host": "localhost",
    "port": "5432"
}

def get_engine():
    return create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

def init_and_upload(p2p_csv_path, trans_csv_path):
    engine = get_engine()
    
    # Структура таблиц приведена к нижнему регистру для синхронизации с Pandas
    create_tables_sql = """
    -- Таблица P2P переводов
    CREATE TABLE IF NOT EXISTS p2p_log (
        id SERIAL PRIMARY KEY,
        userid VARCHAR(255) NOT NULL,
        eventtime TIMESTAMP NOT NULL,
        recipientid VARCHAR(255) NOT NULL,
        amount NUMERIC(15, 2) NOT NULL,
        currency VARCHAR(10) NOT NULL,
        isfraud BOOLEAN NOT NULL
    );

    -- Таблица общих транзакций (с userid)
    CREATE TABLE IF NOT EXISTS trans_log (
        id SERIAL PRIMARY KEY,
        userid VARCHAR(255) NOT NULL,
        eventtime TIMESTAMP NOT NULL,
        cardid VARCHAR(255),
        merchantid INTEGER,
        merchanttype VARCHAR(255),
        amount NUMERIC(15, 2) NOT NULL,
        currency VARCHAR(10) NOT NULL,
        country VARCHAR(10),
        transactionsuccessful BOOLEAN NOT NULL
    );
    
    -- Индексы для ускорения JOIN-объединений
    CREATE INDEX IF NOT EXISTS idx_p2p_user ON p2p_log(userid);
    CREATE INDEX IF NOT EXISTS idx_trans_user ON trans_log(userid);
    """
    
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(create_tables_sql))  
            
    print("Структура таблиц в PostgreSQL успешно подготовлена.")
    
    # --- ЗАГРУЗКА P2P ЛОГОВ ---
    print(f"Загрузка {p2p_csv_path} в базу данных...")
    df_p2p = pd.read_csv(p2p_csv_path, sep=None, engine='python', encoding='utf-8-sig')
    
    if 'Unnamed: 0' in df_p2p.columns:
        df_p2p = df_p2p.drop(columns=['Unnamed: 0'])
        
    df_p2p.columns = df_p2p.columns.str.strip().str.lower()
    
    if 'userid' in df_p2p.columns:
        df_p2p = df_p2p.dropna(subset=['userid'])
    
    if 'isfraud' in df_p2p.columns:
        df_p2p['isfraud'] = df_p2p['isfraud'].astype(bool)
        
    df_p2p.to_sql('p2p_log', engine, if_exists='append', index=False)
    print("P2P логи успешно загружены.")
    
    # --- ЗАГРУЗКА ТРАНЗАКЦИЙ ---
    print(f"Загрузка {trans_csv_path} в базу данных...")
    # Автоматическое определение разделителя решает проблему склеивания колонок
    df_trans = pd.read_csv(trans_csv_path, sep=None, engine='python', encoding='utf-8-sig')
    
    if 'Unnamed: 0' in df_trans.columns:
        df_trans = df_trans.drop(columns=['Unnamed: 0'])
        
    df_trans.columns = df_trans.columns.str.strip().str.lower()

    if 'userid' in df_trans.columns:
        initial_count = len(df_trans)
        df_trans = df_trans.dropna(subset=['userid'])
        print(f"Удалено {initial_count - len(df_trans)} строк с пустым userid.")
    else:
        print("Предупреждение: колонка userid не найдена после разбора файла.")

    if 'transactionsuccessful' in df_trans.columns:
        df_trans['transactionsuccessful'] = df_trans['transactionsuccessful'].astype(bool)
        
    print("Запись транзакций в PostgreSQL (это может занять некоторое время)...")
    df_trans.to_sql('trans_log', engine, if_exists='append', index=False)
    
    print("Все данные успешно импортированы в PostgreSQL!")

if __name__ == "__main__":
    init_and_upload('final_p2p_log.csv', 'final_trans_log.csv')