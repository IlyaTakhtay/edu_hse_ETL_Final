from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import sys, os
import logging

# Добавляем путь к общим модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl_metadata import init_etl_metadata_table, get_last_timestamp, update_etl_metadata

# Параметры DAG
SOURCE_COLLECTION = 'user_sessions'
TARGET_TABLE = 'user_sessions'
TIMESTAMP_FIELD = 'start_time'

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def create_target_table():
    """Создает таблицу user_sessions в PostgreSQL, если она не существует"""
    postgres_schema = os.getenv('POSTGRES_SCHEMA')
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            pages_visited JSONB,
            device JSONB,
            actions JSONB,
            etl_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
        

def extract_data():
    # Получаем последний обработанный timestamp
    last_timestamp = get_last_timestamp(SOURCE_COLLECTION)
    logging.info(f"Last processed timestamp: {last_timestamp}")
    
    # Запрос только новых данных
    mongo_hook = MongoHook(mongo_conn_id='etl_mongo_conn')
    client = mongo_hook.get_conn()
    db_name = os.getenv("MONGO_DB", "ecomm_db")
    collection = client[db_name][SOURCE_COLLECTION]
    sessions = list(collection.find({TIMESTAMP_FIELD: {"$gt": last_timestamp}}, {"_id": 0}))
    
    return sessions, last_timestamp


def transform_data(ti):
    """Преобразует данные из MongoDB в формат, подходящий для PostgreSQL"""
    sessions, last_timestamp = ti.xcom_pull(task_ids='extract_data')
    if not sessions:
        return [], last_timestamp
    
    max_timestamp = last_timestamp  # Инициализируем максимальным известным
    rows = []
    
    for session in sessions:
        # Обновляем максимальный timestamp
        if session.get('start_time') and session.get('start_time') > max_timestamp:
            max_timestamp = session.get('start_time')
            
        # Преобразуем вложенные структуры в JSON для PostgreSQL
        row = (
            session.get('session_id'),
            session.get('user_id'),
            session.get('start_time'),
            session.get('end_time'),
            json.dumps(session.get('pages_visited', [])),
            json.dumps(session.get('device', {})),
            json.dumps(session.get('actions', []))
        )
        rows.append(row)
    
    print(f"Transformed {len(rows)} records for PostgreSQL")
    return rows, max_timestamp

def load_data(ti):
    """Загружает данные в PostgreSQL"""
    rows, max_timestamp = ti.xcom_pull(task_ids='transform_data')
    if not rows:
        return 0
    
    # Загружаем данные в PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.insert_rows(
        table=TARGET_TABLE,
        rows=rows,
        target_fields=[
            'session_id', 'user_id', 'start_time', 'end_time', 
            'pages_visited', 'device', 'actions'
        ],
        replace=True,
        replace_index='session_id'
    )
    
    # Обновляем метаданные ETL с новым максимальным timestamp
    update_etl_metadata(SOURCE_COLLECTION, TARGET_TABLE, max_timestamp, len(rows))
    
    print(f"Loaded {len(rows)} records into PostgreSQL")
    return len(rows)


with DAG(
    'user_sessions_etl',
    default_args=default_args,
    description='ETL процесс для пользовательских сессий',
    schedule_interval='@hourly',
    start_date=datetime(2025, 3, 16),
    catchup=False,
) as dag:
    
    init_metadata = PythonOperator(
        task_id='init_metadata',
        python_callable=init_etl_metadata_table,
    )

    create_table = PythonOperator(
        task_id='create_target_table',
        python_callable=create_target_table,
    )

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Определяем порядок выполнения задач
    init_metadata >> create_table >> extract >> transform >> load
