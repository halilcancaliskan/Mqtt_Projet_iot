# main.py
import threading
import time
from queue_manager import dequeue_message, requeue_expired_messages, clean_up_consumed_messages, process_message
from config import MQTT_TOPIC_1, MQTT_TOPIC_2, MQTT_TOPIC_3, RETRY_INTERVAL

# Consumer processing loop


def consumer_thread(topic):
    while True:
        message = dequeue_message(topic)
        if message:
            print(
                f"Dequeued message: {message['message']} from topic: {topic}")
            process_message(message["message"])
        else:
            print(f"No new messages on {topic}")
        time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    # Create consumer threads
    threading.Thread(target=consumer_thread, args=(MQTT_TOPIC_1,)).start()
    threading.Thread(target=consumer_thread, args=(MQTT_TOPIC_2,)).start()
    threading.Thread(target=consumer_thread, args=(MQTT_TOPIC_3,)).start()

    # Periodically requeue expired messages
    while True:
        requeue_expired_messages()
        clean_up_consumed_messages()
        time.sleep(RETRY_INTERVAL)
