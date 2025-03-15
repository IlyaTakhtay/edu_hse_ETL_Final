from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, List
from mongo_connection import MongoConnector, MongoConfig

# Настройка базовых параметров
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
fake = Faker(['ru_RU', 'en_US'])
random.seed(42)
fake.seed_instance(42)

def generate_error_details():
    error_types = [
        "TypeError", "ValueError", "KeyError", "IndexError", "AttributeError", 
        "ConnectionError", "TimeoutError", "PermissionError", "FileNotFoundError"
    ]
    
    error_messages = [
        "Cannot read property '{0}' of undefined",
        "Expected {0} to be instance of {1}",
        "Maximum call stack size exceeded",
        "Connection refused to {0}::{1}",
        "Failed to parse response as JSON",
        "Unexpected token in JSON at position {0}",
        "Network request timeout after {0}ms",
        "Access denied: insufficient permissions for {0}",
        "Object has been disposed",
        "Invalid configuration for module '{0}'"
    ]
    
    error_type = random.choice(error_types)
    message_template = random.choice(error_messages)
    
    placeholders = []
    for i in range(message_template.count("{") // 2 + 1):
        if "{" + str(i) + "}" in message_template:
            if i % 2 == 0:
                placeholders.append(fake.word())
            else:
                placeholders.append(str(random.randint(10, 5000)))
    
    message = message_template.format(*placeholders) if placeholders else message_template
    
    files = [
        "/app/src/main.js", "/app/src/utils/api.js", "/app/src/services/data.js",
        "/app/src/controllers/users.js", "/app/node_modules/express/lib/router.js"
    ]
    
    methods = [
        "getData", "processRequest", "validateInput", "parseResponse", 
        "authenticate", "fetchResults", "updateRecord", "initialize"
    ]
    
    stack_lines = ["Traceback (most recent call last):"]
    depth = random.randint(3, 6)
    
    for i in range(depth):
        file = random.choice(files)
        line = random.randint(10, 500)
        col = random.randint(1, 80)
        method = random.choice(methods)
        
        stack_lines.append(f"  at {method} ({file}:{line}:{col})")
    
    stack_lines.append(f"  at Object.<anonymous> ({random.choice(files)}:{random.randint(10, 500)}:{random.randint(1, 80)})")
    stack_lines.append(f"{error_type}: {message}")
    
    return {
        'code': random.choice([400, 403, 404, 422, 429, 500, 502, 503]),
        'message': f"{error_type}: {message}",
        'stack_trace': "\n".join(stack_lines),
        'timestamp': datetime.now().isoformat()
    }

def generate_base_data() -> tuple:
    """Генерирует базовые сущности для тестовых данных"""
    num_users = int(os.environ.get('AMOUNT_USERS', 100))
    users = [{'user_id': str(uuid.uuid4()), 
              'name': fake.name(), 
              'email': fake.email()} 
             for _ in range(num_users)]
    
    category_names = ['Смартфоны', 'Ноутбуки', 'Наушники', 'Телевизоры', 
                      'Фотоаппараты', 'Умные часы', 'Планшеты', 'Принтеры']
    categories = [{'category_id': str(uuid.uuid4()), 'name': name} for name in category_names]
    
    num_products = int(os.environ.get('AMOUNT_PRODUCTS', 200))
    products = []
    for _ in range(num_products):
        category = random.choice(categories)
        base_price = random.uniform(1000, 100000)
        products.append({
            'product_id': str(uuid.uuid4()),
            'name': f"{category['name']} {fake.word().capitalize()} {random.choice(['Pro', 'Max', 'Ultra', 'Lite', ''])}",
            'category_id': category['category_id'],
            'base_price': round(base_price, 2),
            'current_price': round(base_price * random.uniform(0.8, 1.2), 2)
        })
    
    logger.info(f"Сгенерировано: {len(users)} пользователей, {len(products)} продуктов")
    return users, products

def generate_user_sessions(users: List[Dict], products: List[Dict], count: int = 1000) -> List[Dict]:
    devices = ['Mobile Android', 'Mobile iOS', 'Desktop Windows', 'Desktop Mac', 'Tablet']
    pages = ['home', 'catalog', 'product', 'cart', 'checkout', 'profile', 'orders', 'wishlist']
    actions = ['view', 'click', 'scroll', 'add_to_cart', 'remove_from_cart', 'purchase', 'like']
    
    sessions = []
    for _ in range(count):
        user = random.choice(users)
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        duration = random.randint(1, 120)
        end_time = start_time + timedelta(minutes=duration)
        
        visited_pages = [{'page': random.choice(pages), 'time_spent': random.randint(10, 300)} 
                         for _ in range(random.randint(1, 10))]
        
        user_actions = []
        for _ in range(random.randint(1, 15)):
            action = random.choice(actions)
            details = {'product_id': random.choice(products)['product_id']} if action in ['add_to_cart', 'like'] else {}
            user_actions.append({
                'action_type': action,
                'timestamp': fake.date_time_between(start_time, end_time).isoformat(),
                'details': details
            })
        
        sessions.append({
            'session_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'start_time': start_time,
            'end_time': end_time,
            'pages_visited': visited_pages,
            'device': {
                'type': random.choice(devices),
                'browser': fake.user_agent(),
                'ip': fake.ipv4()
            },
            'actions': sorted(user_actions, key=lambda x: datetime.fromisoformat(str(x['timestamp'])))
        })
    
    return sessions

def generate_price_history(products: List[Dict]) -> List[Dict]:
    currencies = ['RUB', 'USD', 'EUR']
    reasons = ['seasonal', 'promotion', 'cost_change', 'competition']
    
    price_histories = []
    for product in products:
        price_changes = []
        last_price = product['base_price']
        
        for _ in range(random.randint(2, 8)):
            date = fake.date_time_between(start_date='-1y', end_date='-1d')
            new_price = round(last_price * random.uniform(0.8, 1.2), 2)
            
            price_changes.append({
                'date': date,
                'old_price': last_price,
                'new_price': new_price,
                'reason': random.choice(reasons)
            })
            last_price = new_price
        
        price_histories.append({
            'product_id': product['product_id'],
            'price_changes': sorted(price_changes, key=lambda x: x['date']),
            'current_price': product['current_price'],
            'currency': random.choice(currencies)
        })
    
    return price_histories

def generate_event_logs(users: List[Dict], products: List[Dict], count: int = 2000) -> List[Dict]:
    event_types = ['login', 'logout', 'purchase', 'page_view', 'error', 'system_update', 'payment_processed']
    
    events = []
    for _ in range(count):
        event_type = random.choice(event_types)
        
        if event_type == 'login':
            details = {'user_id': random.choice(users)['user_id'], 'ip': fake.ipv4(), 'success': random.random() > 0.05}
        elif event_type == 'logout':
            details = {'user_id': random.choice(users)['user_id'], 'session_duration': random.randint(60, 3600)}
        elif event_type == 'purchase':
            details = {
                'user_id': random.choice(users)['user_id'], 
                'product_id': random.choice(products)['product_id'], 
                'amount': round(random.uniform(100, 10000), 2)
            }
        elif event_type == 'error':
            details = generate_error_details()
        else:
            details = {}
        
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'event_type': event_type,
            'details': details
        })
    
    return events

def generate_support_tickets(users: List[Dict], count: int = 300) -> List[Dict]:
    issue_types = ['account', 'payment', 'delivery', 'product', 'website', 'refund']
    statuses = ['open', 'in_progress', 'waiting_for_customer', 'resolved', 'closed']
    
    tickets = []
    for _ in range(count):
        user_id = random.choice(users)['user_id']
        created_at = fake.date_time_between(start_date='-60d', end_date='-1d')
        
        messages = []
        last_time = created_at
        
        for i in range(random.randint(1, 8)):
            message_time = last_time + timedelta(hours=random.randint(1, 24))
            sender = 'customer' if i % 2 == 0 else 'support'
            
            messages.append({
                'sender': sender,
                'message': fake.paragraph(),
                'timestamp': message_time
            })
            last_time = message_time
        
        tickets.append({
            'ticket_id': str(uuid.uuid4()),
            'user_id': user_id,
            'status': random.choice(statuses),
            'issue_type': random.choice(issue_types),
            'messages': messages,
            'created_at': created_at,
            'updated_at': last_time
        })
    
    return tickets

def generate_user_recommendations(users: List[Dict], products: List[Dict]) -> List[Dict]:
    reasons = ['similar_purchase', 'popular_in_category', 'frequently_bought_together', 'browsing_history']
    
    recommendations = []
    for user in users:
        recommended_items = []
        for _ in range(random.randint(5, 20)):
            recommended_items.append({
                'product_id': random.choice(products)['product_id'],
                'score': round(random.uniform(0.1, 1.0), 2),
                'reason': random.choice(reasons)
            })
        
        recommendations.append({
            'user_id': user['user_id'],
            'recommended_products': recommended_items,
            'last_updated': fake.date_time_between(start_date='-7d', end_date='now')
        })
    
    return recommendations

def generate_moderation_queue(users: List[Dict], products: List[Dict], count: int = 500) -> List[Dict]:
    statuses = ['pending', 'approved', 'rejected', 'needs_revision']
    flags = ['spam', 'offensive', 'irrelevant', 'fake', 'contains_personal_info']
    
    reviews = []
    for _ in range(count):
        # 30% отзывов имеют флаги
        review_flags = random.sample(flags, k=random.randint(1, 3)) if random.random() < 0.3 else []
        
        reviews.append({
            'review_id': str(uuid.uuid4()),
            'user_id': random.choice(users)['user_id'],
            'product_id': random.choice(products)['product_id'],
            'review_text': fake.paragraph(),
            'rating': random.randint(1, 5),
            'moderation_status': random.choice(statuses),
            'flags': review_flags,
            'submitted_at': fake.date_time_between(start_date='-90d', end_date='now')
        })
    
    return reviews

def generate_search_queries(users: List[Dict], count: int = 800) -> List[Dict]:
    filters = ['price_asc', 'price_desc', 'rating', 'newest', 'popularity', 'discount']
    search_terms = ['смартфон', 'ноутбук', 'наушники', 'телевизор', 'фотоаппарат', 'часы', 'планшет', 'принтер']
    
    queries = []
    for _ in range(count):
        applied_filters = random.sample(filters, k=random.randint(1, 3)) if random.random() < 0.7 else []
        user_id = random.choice(users)['user_id'] if random.random() < 0.8 else None
        
        queries.append({
            'query_id': str(uuid.uuid4()),
            'user_id': user_id,
            'query_text': random.choice(search_terms) + ' ' + fake.word(),
            'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'filters': applied_filters,
            'results_count': random.randint(0, 200)
        })
    
    return queries

def save_to_mongodb(db, collection_name: str, data: List[Dict]) -> None:
    if not data:
        logger.warning(f"Нет данных для сохранения в {collection_name}")
        return
    
    db[collection_name].drop()
    db[collection_name].insert_many(data)
    logger.info(f"Сохранено {len(data)} записей в коллекцию {collection_name}")

def main():
    try:
        config = MongoConfig()
        db_connector = MongoConnector(config=config)
        db = db_connector.connect()
        
        users, products = generate_base_data()
        
        generators = [
            ('user_sessions', lambda: generate_user_sessions(users, products)),
            ('product_price_history', lambda: generate_price_history(products)),
            ('event_logs', lambda: generate_event_logs(users, products)),
            ('support_tickets', lambda: generate_support_tickets(users)),
            ('user_recommendations', lambda: generate_user_recommendations(users, products)),
            ('moderation_queue', lambda: generate_moderation_queue(users, products)),
            ('search_queries', lambda: generate_search_queries(users))
        ]
        
        for collection_name, generator_func in generators:
            save_to_mongodb(db, collection_name, generator_func())
        
        logger.info("Генерация данных завершена успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при генерации данных: {str(e)}")
        raise

if __name__ == "__main__":
    main()
