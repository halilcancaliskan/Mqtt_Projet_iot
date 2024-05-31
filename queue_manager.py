# queue_manager.py
from pymongo import MongoClient
from config import MONGO_URI, MESSAGE_TTL, RETRY_INTERVAL
from datetime import datetime, timedelta
import time

client = MongoClient(MONGO_URI)
db = client["mqtt"]
collection = db["message_queue"]


def enqueue_message(topic, message):
    collection.insert_one({
        "topic": topic,
        "message": message,
        "consumed": False,
        "timestamp": datetime.utcnow()
    })
    print(f"Enqueued message: {message} to topic: {topic}")


def dequeue_message(topic):
    message = collection.find_one_and_update(
        {"topic": topic, "consumed": False},
        {"$set": {"consumed": True}},
        sort=[('timestamp', 1)]
    )
    if message:
        print(f"Dequeued message: {message}")
    return message


def requeue_message(message_id):
    collection.update_one(
        {"_id": message_id},
        {"$set": {"consumed": False, "timestamp": datetime.utcnow()}}
    )
    print(f"Requeued message with ID: {message_id}")


def clean_up_consumed_messages():
    result = collection.delete_many({"consumed": True})
    print(f"Cleaned up {result.deleted_count} consumed messages")


def requeue_expired_messages():
    expiry_time = datetime.utcnow() - timedelta(seconds=MESSAGE_TTL)
    expired_messages = collection.find(
        {"consumed": True, "timestamp": {"$lt": expiry_time}})
    for message in expired_messages:
        requeue_message(message["_id"])


def clear_collection():
    result = collection.delete_many({})
    print(f"Cleared collection, deleted {result.deleted_count} documents")


def process_message(message):
    # Simulate message processing
    print(f"Processing message: {message}")


def handle_message(topic):
    while True:
        message = dequeue_message(topic)
        if message:
            process_message(message["message"])
        else:
            print(f"No new messages on {topic}")
        time.sleep(RETRY_INTERVAL)
