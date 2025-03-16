CREATE TABLE IF NOT EXISTS etl_metadata (
    source_collection VARCHAR(100) PRIMARY KEY,  -- Имя источника (коллекции MongoDB)
    target_table VARCHAR(100),                   -- Имя целевой таблицы в PostgreSQL
    last_timestamp TIMESTAMP,                    -- Последняя обработанная временная метка
    last_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP,-- Время последнего запуска ETL
    records_processed BIGINT DEFAULT 0,          -- Кол-во обработанных записей всего
    last_batch_count INTEGER DEFAULT 0,          -- Кол-во записей в последнем запуске
    status VARCHAR(50) DEFAULT 'success'         -- Статус (success/failed)
);
