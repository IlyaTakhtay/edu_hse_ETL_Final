
import os
from dataclasses import dataclass
from pymongo import MongoClient, database
import logging as logger
import time

@dataclass
class MongoConfig:
    """Конфигурация подключения к MongoDB"""
    host: str = os.environ.get('MONGO_HOST', 'mongodb')
    port: int = int(os.environ.get('MONGO_PORT', 27017))
    username: str = os.environ.get('MONGO_ROOT_USERNAME', 'admin')
    password: str = os.environ.get('MONGO_ROOT_PASSWORD', 'admin123')
    db_name: str = os.environ.get('MONGO_DB', 'ecommerce_db')
    
    @property
    def uri(self) -> str:
        return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"


class MongoConnector:
    """Класс для подключения к MongoDB"""
    def __init__(self, config: MongoConfig):
        self.config: MongoConfig = config
        self.client: MongoClient
        self.db = None
    
    def connect(self):
        """Подключение к MongoDB с повторными попытками"""
        max_attempts = 5
        sleep_time = 2
        for attempt in range(max_attempts):
            try:
                self.client = MongoClient(self.config.uri)
                self.client.admin.command('ping')
                self.db = self.client[self.config.db_name]
                logger.info(f"Подключено к MongoDB, база данных: {self.config.db_name}")
                return self.db
            except Exception as e:
                logger.warning(f"Попытка {attempt+1}/{max_attempts}: MongoDB недоступна. Ожидание {sleep_time} секунд... ({str(e)})")
                time.sleep(sleep_time)
        
        raise ConnectionError("Не удалось подключиться к MongoDB после нескольких попыток")
    
    def drop_collections(self, collections: list[str]) -> None:
        """Очистка коллекций перед генерацией данных"""
        for collection in collections:
            if self.db is not None:
                self.db[collection].drop()
            logger.info(f"Коллекция {collection} очищена")
