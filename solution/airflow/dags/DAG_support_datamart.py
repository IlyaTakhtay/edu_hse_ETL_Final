import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1)
}

def create_review_mart_table():
    """Создает таблицу датамарта"""
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    hook.run("""
        CREATE TABLE IF NOT EXISTS etl.review_mart (
            product_id VARCHAR(255),
            total_reviews INT,
            avg_rating FLOAT,
            approved_reviews_ratio FLOAT,
            total_flags INT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (product_id)
        );
    """)

def process_review_data(**context):
    """Извлечение и обработка данных из moderation_queue"""
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Получение всех данных из таблицы moderation_queue
    query = """
    SELECT 
        product_id,
        review_id,
        rating,
        moderation_status,
        flags
    FROM 
        moderation_queue
    """
    
    # Логируем сам запрос для диагностики
    logging.info("Executing query: %s", query)
    
    # Извлечение данных в Pandas DataFrame
    df = hook.get_pandas_df(query)
    
    # Логируем количество извлеченных записей и первые несколько строк данных
    logging.info("Number of records extracted: %d", len(df))
    if not df.empty:
        logging.info("Sample data extracted:\n%s", df.head().to_string())
    else:
        logging.warning("No data found in moderation_queue.")
    
    if df.empty:
        return pd.DataFrame()  # Если данных нет, возвращаем пустой DataFrame
    
    # Подсчет количества флагов
    df['flags_count'] = df['flags'].apply(lambda x: len(x) if isinstance(x, list) else 0)
    
    # Агрегация данных по продуктам
    aggregated = df.groupby('product_id').agg(
        total_reviews=('review_id', 'count'),
        avg_rating=('rating', 'mean'),
        approved_reviews_ratio=('moderation_status', lambda x: (x == 'approved').mean()),
        total_flags=('flags_count', 'sum')
    ).reset_index()
    
    return aggregated

def load_review_mart(**context):
    """Загрузка обработанных данных в таблицу датамарта"""
    df = context['ti'].xcom_pull(task_ids='process_review_data')
    
    if df.empty:
        logging.warning("No data to load into review_mart.")
        return "No data to load"
    
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Вставка данных в таблицу review_mart
    hook.insert_rows(
        table='etl.review_mart',
        rows=df[['product_id', 'total_reviews', 'avg_rating', 'approved_reviews_ratio', 'total_flags']].values.tolist(),
        target_fields=['product_id', 'total_reviews', 'avg_rating', 'approved_reviews_ratio', 'total_flags'],
        replace=True,  # Заменяем существующие записи
        replace_index=['product_id']  # Указываем уникальный индекс
    )
    
    logging.info("Loaded %d records into review_mart.", len(df))
    
    return f"Loaded {len(df)} records into review_mart"


with DAG(
    'review_data_mart_full_period',
    default_args=default_args,
    schedule_interval=None,  # Запуск вручную для обработки всех данных за весь период
    catchup=False
) as dag:
    
    create_table_task = PythonOperator(
        task_id='create_review_mart_table',
        python_callable=create_review_mart_table
    )

    process_data_task = PythonOperator(
        task_id='process_review_data',
        python_callable=process_review_data,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id='load_review_mart',
        python_callable=load_review_mart,
        provide_context=True
    )

create_table_task >> process_data_task >> load_data_task
