from pykafka import KafkaClient
import json
propertiesFile = open('properties.json')
properties = json.load(propertiesFile)

KafkaBrokers=properties["Kafka_brokers"]
client = KafkaClient(hosts=KafkaBrokers)
kafka_topic_name=properties["Kafka_destination_topic1"] #"dest"
for i in client.topics[kafka_topic_name].get_simple_consumer():
	x = i.value.decode()
	print('data:{0}\n'.format(i.value.decode()))
print("*************************************************")
	#try:
	#	final_dictionary = json.loads(x) 
	#	print(final_dictionary["key"] + "     " + final_dictionary["timestamp"])
	#except:
	#	print("")