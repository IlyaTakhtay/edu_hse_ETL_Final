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
SOURCE_COLLECTION = 'moderation_queue'
TARGET_TABLE = 'moderation_queue'
TIMESTAMP_FIELD = 'submitted_at'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
}

def create_target_table():
    """Создает таблицу moderation_queue в PostgreSQL, если она не существует"""
    postgres_schema = os.getenv('POSTGRES_SCHEMA')
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS moderation_queue (
            review_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            review_text TEXT,
            rating INTEGER,
            moderation_status VARCHAR(50),
            flags JSONB,
            submitted_at TIMESTAMP,
            etl_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def extract_data():
    """Извлекает данные из MongoDB"""
    # Получаем последний обработанный timestamp
    last_timestamp = get_last_timestamp(SOURCE_COLLECTION)
    logging.info(f"Last processed timestamp: {last_timestamp}")
    
    # Запрос только новых данных
    mongo_hook = MongoHook(mongo_conn_id='etl_mongo_conn')
    client = mongo_hook.get_conn()
    db_name = os.getenv("MONGO_DB", "ecomm_db")
    collection = client[db_name][SOURCE_COLLECTION]
    reviews = list(collection.find({TIMESTAMP_FIELD: {"$gt": last_timestamp}}, {"_id": 0}))
    
    return reviews, last_timestamp

def transform_data(ti):
    """Преобразует данные из MongoDB в формат, подходящий для PostgreSQL"""
    reviews, last_timestamp = ti.xcom_pull(task_ids='extract_data')
    if not reviews:
        return [], last_timestamp
    
    max_timestamp = last_timestamp  # Инициализируем максимальным известным
    rows = []
    
    for review in reviews:
        # Обновляем максимальный timestamp
        if review.get('submitted_at') and review.get('submitted_at') > max_timestamp:
            max_timestamp = review.get('submitted_at')
            
        # Преобразуем вложенные структуры в JSON для PostgreSQL
        row = (
            review.get('review_id'),
            review.get('user_id'),
            review.get('product_id'),
            review.get('review_text'),
            review.get('rating'),
            review.get('moderation_status'),
            json.dumps(review.get('flags', [])),
            review.get('submitted_at')
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
            'review_id', 'user_id', 'product_id', 'review_text', 
            'rating', 'moderation_status', 'flags', 'submitted_at'
        ],
        replace=True,
        replace_index='review_id'
    )
    
    # Обновляем метаданные ETL с новым максимальным timestamp
    update_etl_metadata(SOURCE_COLLECTION, TARGET_TABLE, max_timestamp, len(rows))
    
    print(f"Loaded {len(rows)} records into PostgreSQL")
    return len(rows)

with DAG(
    'moderation_queue_etl',
    default_args=default_args,
    description='ETL процесс для очереди модерации отзывов',
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
