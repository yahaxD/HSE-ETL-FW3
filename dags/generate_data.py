import os
import random
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient, errors
from faker import Faker
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

fake = Faker()
record_count = round(random.normalvariate(1000, 200))
user_count = round(random.normalvariate(400, 100))
product_count = round(random.normalvariate(100, 50))
User_id = [x for x in range(1000)]
Product_id = [x for x in range(500)]
Page = ["/home", "/about", "/contact", "/products", "/cart", "/checkout", "/payment", "/profile"]
Device = [{"type": "mobile", "os": "Android"}, {"type": "mobile", "os": "IOS"}, {"type": "PC", "os": "Windows"},
          {"type": "PC", "os": "Linux"}, {"type": "PC", "os": "MacOS"}]
Action = ["click", "scroll", "add_to_cart", "login", "logout", "search", "purchase"]
Currency = ["USD", "EUR", "RUB"]
Event = ["error", "system_alert", "maintenance", "warning"]
Ticket_status = ["opened", "closed", "in_progress", "resolved", "rejected"]
Issue_type = ["login_issue", "payment_issue", "technical_issue", "account_issue", "other"]
Moderation_status = ["under_consideration", "approved", "rejected"]


def generate_user_session() -> dict:
    start_time = fake.date_time_this_year()
    return {
        "session_id": str(uuid.uuid4()),
        "user_id": random.choice(User_id),
        "start_time": str(start_time),
        "end_time": str(start_time + timedelta(minutes=random.randint(1, 60)) + timedelta(seconds=random.randint(1, 60))),
        "pages_visited": [random.choice(Page) for _ in range(random.randint(1, 10))],
        "device": random.choice(Device),
        "actions": [random.choice(Page) for _ in range(round(random.normalvariate(7, 4)))],
    }


def generate_product_price_history() -> dict:
    return {
        "product_id": random.choice(Product_id),
        "price_changes": [round(random.uniform(10, 1000), 2) for _ in range(random.randint(1, 10))],
        "current_price": round(random.uniform(10, 1000), 2),
        "currency": random.choice(Currency)
    }


def generate_event_log() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": str(fake.date_time_this_year()),
        "event_type": random.choice(Event),
        "details": fake.text()
    }


def generate_support_ticket() -> dict:
    creation_time = fake.date_time_this_year()
    return {
        "ticket_id ": str(uuid.uuid4()),
        "user_id": random.choice(User_id),
        "status": random.choice(Ticket_status),
        "issue_type": random.choice(Issue_type),
        "message": [fake.text() for _ in range(random.randint(1, 10))],
        "created_at": str(creation_time),
        "updated_at ": str(fake.date_time_between(start_date=creation_time))
    }


def generate_user_recommendation() -> dict:
    return {
        "user_id": random.choice(User_id),
        "recommended_products ": [random.choice(Product_id) for _ in range(random.randint(3, 10))],
        "last_updated": str(fake.date_time_this_year())
    }


def generate_moderation_queue() -> dict:
    return {
        "review_id": str(uuid.uuid4()),
        "user_id": random.choice(User_id),
        "product_id": random.choice(Product_id),
        "review_text": fake.text(),
        "rating": random.randint(1, 10),
        "moderation_status": random.choice(Moderation_status),
        "flags": [fake.word() for _ in range(random.randint(1, 10))],
        "submitted_at": str(fake.date_time_this_year())
    }


def generate_search_query() -> dict:
    return {
        "query_id": str(uuid.uuid4()),
        "user_id": random.choice(User_id),
        "query_text": fake.text(100),
        "timestamp": str(fake.date_time_this_year()),
        "filters": [fake.word() for _ in range(random.randint(1, 10))],
        "result_count": random.randint(1, 50)
    }


def generate_data():

    # MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:root@localhost:27017")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:root@mongodb:27017")
    DB_NAME = os.getenv("MONGO_DB", "etl_database")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        client.server_info()
        print("MongoDB connected")
    except errors.ServerSelectionTimeoutError:
        print("MongoDB connection error")
        exit(1)

    for _ in range(record_count):
        user_session = generate_user_session()
        product_price_history = generate_product_price_history()
        event_log = generate_event_log()
        support_ticket = generate_support_ticket()
        user_recommendation = generate_user_recommendation()
        moderation_queue = generate_moderation_queue()
        search_query = generate_search_query()

        db.user_sessions.insert_one(user_session)
        db.product_price_history.insert_one(product_price_history)
        db.event_logs.insert_one(event_log)
        db.support_tickets.insert_one(support_ticket)
        db.user_recommendations.insert_one(user_recommendation)
        db.moderation_queue.insert_one(moderation_queue)
        db.search_queries.insert_one(search_query)

    print("Data inserted successfully")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "data_initialize",
    default_args=default_args,
    description="DAG for data initialization in MongoDB",
    catchup=False,
)

initialize_data_task = PythonOperator(
    task_id="generate_and_load_data",
    python_callable=generate_data,
    dag=dag,
)

initialize_data_task
