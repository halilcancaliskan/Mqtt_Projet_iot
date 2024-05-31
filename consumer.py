# consumer.py
import paho.mqtt.client as mqtt
import json
from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_1, MQTT_TOPIC_2, MQTT_TOPIC_3
from queue_manager import enqueue_message, clear_collection

# Nettoyer la collection au démarrage du consumer
clear_collection()

# MQTT Broker details
broker = MQTT_BROKER
port = MQTT_PORT

# Topics
topics = [MQTT_TOPIC_1, MQTT_TOPIC_2, MQTT_TOPIC_3]

# Consumer callback functions


def on_message(client, userdata, message):
    payload = json.loads(message.payload)
    print(f"Received message: {payload}")
    # Enqueue message
    enqueue_message(message.topic, payload)


def on_connect(client, userdata, flags, reasonCode, properties=None):
    print("Connected to MQTT Broker")
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")

# Consumer loop


def consumer_loop():
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    client.loop_forever()


if __name__ == "__main__":
    consumer_loop()
