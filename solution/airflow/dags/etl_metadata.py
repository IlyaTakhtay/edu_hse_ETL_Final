from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def init_etl_metadata_table():
    """Создает таблицу метаданных ETL, если забыли накатить миграцию"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS etl_metadata (
            source_collection VARCHAR(100) PRIMARY KEY,
            target_table VARCHAR(100) NOT NULL,
            last_timestamp TIMESTAMP,
            last_run TIMESTAMP,
            records_processed BIGINT DEFAULT 0,
            last_batch_count INTEGER DEFAULT 0,
            status VARCHAR(50)
        );
    """)

def get_last_timestamp(collection_name):
    """Получает последнюю обработанную временную метку для указанной коллекции"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    result = pg_hook.get_records(
        "SELECT last_timestamp FROM etl_metadata WHERE source_collection = %s",
        (collection_name,)
    )
    if result and result[0][0]:
        return result[0][0]
    return datetime(1970, 1, 1)

def update_etl_metadata(source_collection, target_table, max_timestamp, batch_count):
    """Обновляет метаданные ETL после успешной загрузки"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Insert or update ETL metadata
    pg_hook.run("""
        INSERT INTO etl_metadata 
        (source_collection, target_table, last_timestamp, last_run, last_batch_count, records_processed, status) 
        VALUES (%s, %s, %s, NOW(), %s, %s, %s)
        ON CONFLICT (source_collection) DO UPDATE SET 
            target_table = EXCLUDED.target_table,
            last_timestamp = EXCLUDED.last_timestamp,
            last_run = EXCLUDED.last_run,
            last_batch_count = EXCLUDED.last_batch_count,
            records_processed = etl_metadata.records_processed + EXCLUDED.last_batch_count,
            status = EXCLUDED.status
    """, parameters=[
        source_collection,  # collection name
        target_table,       # table name
        max_timestamp,      # timestamp
        batch_count,        # last_batch_count
        batch_count,        # initial records_processed (for new entries)
        'success'           # status
    ])
    
    print(f"ETL metadata updated: last timestamp = {max_timestamp}, processed {batch_count} records")

