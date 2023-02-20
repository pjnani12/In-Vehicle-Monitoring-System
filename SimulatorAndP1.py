import paho.mqtt.client as paho #mqtt library
import os
import json
import time
from datetime import datetime
import random

propertiesFile = open('properties.json')
properties = json.load(propertiesFile)
broker=properties["MQTT_broker"]#"localhost" #host name , Replace with your IP address.
topic=properties["MQTT_topic"]#"test";#topic name
port=properties["MQTT_port"]#1883 #MQTT data listening port
vechicleid="vehicle001"
driverId="driver001"

print("mqtt_broker:::"+broker)
print("mqtt_topic_name:::"+topic)
print("MQTT_port:::"+str(port))

def getCoordinates(routeId):
		latPrefix = 32
		longPrefix = 95
		if (routeId == 0):
			latPrefix = 33
			longPrefix = 96
		elif (routeId == 1):
			latPrefix = 34
			longPrefix = 97
		elif (routeId == 2):
			latPrefix = 35
			longPrefix = 98
		elif (routeId == 3):
			latPrefix = 36
			longPrefix = 99
		elif (routeId == 4):
			latPrefix = 37
			longPrefix = 99

		lati = latPrefix + random.random()
		longi = longPrefix + random.random()
		return lati , longi

def on_publish(client,userdata,result): #create function for callback
  print("published data is : ")
  pass

client1= paho.Client("control1") #create client object
client1.on_publish = on_publish #assign function to callback
#client1.username_pw_set(ACCESS_TOKEN) #access token from thingsboard device
client1.connect(broker,port,keepalive=60) #establishing connection

data = {}
data['vechicleid'] = vechicleid
data['driverId'] = driverId

#publishing after every 10 secs
i=0
while i < 1000:
  data['timestamp'] = str(datetime.utcnow())
  routeId = (int(i/20))%5
  coords = getCoordinates(routeId)
  data['latitude'] = coords[0]
  data['longitude'] = coords[1]
  if i % 10 == 0 :
      data['speed'] = 60 #Over speeding
  else :
      data['speed'] = 30
  message = str(json.dumps(data))
  print(message)
  ret= client1.publish(topic,message) #topic name is test
  print(i)
  i=i+1
  print("MQTT broker sent data to topic:"+ topic)
  time.sleep(5)
  
