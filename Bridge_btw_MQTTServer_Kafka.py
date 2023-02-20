import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time
import json

propertiesFile = open('properties.json')
properties = json.load(propertiesFile)
mqtt_topic_name=properties["MQTT_topic"] #"test"
kafka_topic_name=properties["Kafka_source_topic"] #"test"
#mqtt_broker = "mqtt.eclipseprojects.io"
mqtt_broker =  properties["MQTT_broker"]#"localhost"
port=properties["MQTT_port"] #1883 #MQTT data listening port
KafkaBrokers=properties["Kafka_brokers"]

print("mqtt_broker:::"+mqtt_broker)
print("mqtt_topic_name:::"+mqtt_topic_name)
print("MQTT_port:::"+str(port))
print("KafkaBrokers:::"+KafkaBrokers)
print("kafka_topic_name:::"+kafka_topic_name)

mqtt_client = mqtt.Client(mqtt_topic_name)
mqtt_client.connect(mqtt_broker,port,keepalive=60)

# kafka_client = KafkaClient(hosts="localhost:9092")
kafka_client = KafkaClient(hosts=KafkaBrokers)
kafka_topic = kafka_client.topics[kafka_topic_name]
kafka_producer = kafka_topic.get_sync_producer()

def on_message(client, userdata, message):
    # msg_payload = str(message.payload)
    msg_payload = str(message.payload.decode("utf-8"))
    print("Received MQTT message: ", msg_payload)
    kafka_producer.produce(msg_payload.encode('ascii'))
    print("KAFKA: Just published " + msg_payload + " to topic " + kafka_topic_name)

mqtt_client.loop_start()
# mqtt_client.subscribe("test")
mqtt_client.subscribe(mqtt_topic_name)
mqtt_client.on_message = on_message
# time.sleep(300)
# mqtt_client.loop_end()
while True:
    time.sleep(1)