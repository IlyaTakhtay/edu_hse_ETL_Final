from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1)
}

with DAG(
    'support_efficiency_mart',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Создание таблицы витрины
    create_mart_table = PostgresOperator(
        task_id='create_mart_table',
        postgres_conn_id='postgres_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS etl.support_efficiency_mart (
            date_id DATE,
            agent_id VARCHAR(50),
            issue_type VARCHAR(50),
            tickets_count INT,
            first_response_time_minutes FLOAT,
            avg_response_time_minutes FLOAT,
            resolution_time_hours FLOAT,
            interactions_count FLOAT,
            PRIMARY KEY (date_id, agent_id, issue_type)
        );
        """
    )

    # Извлечение и обработка данных
    def process_tickets_data(**context):
        # Подключение к MongoDB
        mongo_hook = MongoHook(mongo_conn_id='mongo_conn')
        
        # Получение тикетов за предыдущие сутки
        yesterday = context['execution_date'] - timedelta(days=1)
        tickets = mongo_hook.find(
            mongo_collection='tickets',
            query={
                'created_at': {'$gte': yesterday, '$lt': context['execution_date']}
            },
            mongo_db='ecomm_db'
        )
        
        # Расчет метрик для каждого тикета
        metrics = []
        for ticket in tickets:
            # Нахождение первого ответа поддержки
            support_messages = [m for m in ticket['messages'] if m['sender'] == 'support']
            customer_messages = [m for m in ticket['messages'] if m['sender'] == 'customer']
            
            if support_messages:
                first_response_time = (support_messages[0]['timestamp'] - ticket['created_at']).total_seconds() / 60
                
                # Расчет среднего времени ответа
                response_times = []
                for i, msg in enumerate(customer_messages):
                    for support_msg in support_messages:
                        if support_msg['timestamp'] > msg['timestamp']:
                            response_times.append((support_msg['timestamp'] - msg['timestamp']).total_seconds() / 60)
                            break
                
                avg_response_time = sum(response_times) / len(response_times) if response_times else 0
                
                # Время разрешения (если тикет закрыт)
                resolution_time = 0
                if ticket['status'] == 'closed':
                    resolution_time = (ticket['updated_at'] - ticket['created_at']).total_seconds() / 3600
                
                # Другие метрики
                metrics.append({
                    'date_id': yesterday.date(),
                    'agent_id': support_messages[0]['agent_id'] if 'agent_id' in support_messages[0] else 'unknown',
                    'issue_type': ticket['issue_type'],
                    'first_response_time': first_response_time,
                    'avg_response_time': avg_response_time,
                    'resolution_time': resolution_time,
                    'interactions_count': len(ticket['messages'])
                })
        
        # Агрегация по агентам и типам проблем
        return metrics

    transform_data = PythonOperator(
        task_id='transform_ticket_data',
        python_callable=process_tickets_data,
        provide_context=True
    )

    # Загрузка данных в витрину
    def load_metrics_to_mart(**context):
        metrics = context['ti'].xcom_pull(task_ids='transform_ticket_data')
        
        if not metrics:
            return "No data to load"
            
        # Агрегация метрик
        df = pd.DataFrame(metrics)
        aggregated = df.groupby(['date_id', 'agent_id', 'issue_type']).agg({
            'first_response_time': 'mean',
            'avg_response_time': 'mean',
            'resolution_time': 'mean',
            'interactions_count': 'mean'
        }).reset_index()
        
        # Добавление подсчета тикетов
        counts = df.groupby(['date_id', 'agent_id', 'issue_type']).size().reset_index(name='tickets_count')
        aggregated = aggregated.merge(counts, on=['date_id', 'agent_id', 'issue_type'])
        
        # Загрузка в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        pg_hook.insert_rows(
            table='etl.support_efficiency_mart',
            rows=aggregated.values.tolist(),
            target_fields=[
                'date_id', 'agent_id', 'issue_type', 'tickets_count',
                'first_response_time_minutes', 'avg_response_time_minutes',
                'resolution_time_hours', 'interactions_count'
            ],
            replace=True,
            replace_index=['date_id', 'agent_id', 'issue_type']
        )
        
        return f"Loaded {len(aggregated)} records into support efficiency mart"

    load_mart = PythonOperator(
        task_id='load_support_mart',
        python_callable=load_metrics_to_mart,
        provide_context=True
    )

    # Последовательность задач
    create_mart_table >> transform_data >> load_mart
