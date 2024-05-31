# publisher.py
import paho.mqtt.client as mqtt
import json
from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_1, MQTT_TOPIC_2, MQTT_TOPIC_3

# MQTT Broker details
broker = MQTT_BROKER
port = MQTT_PORT

# Topics
topics = [MQTT_TOPIC_1, MQTT_TOPIC_2, MQTT_TOPIC_3]

# MQTT Publisher


def publish_message(topic, message):
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.connect(broker, port)
    client.publish(topic, json.dumps(message))
    client.disconnect()
    print(f"Published message: {message} to topic: {topic}")


# Example messages
messages = [
    {"id": 1, "content": "Message for topic1"},
    {"id": 2, "content": "Message for topic2"},
    {"id": 3, "content": "Message for topic3"}
]

# Publish messages to respective topics
publish_message(MQTT_TOPIC_1, messages[0])
publish_message(MQTT_TOPIC_2, messages[1])
publish_message(MQTT_TOPIC_3, messages[2])
