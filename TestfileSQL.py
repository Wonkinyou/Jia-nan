
import paho.mqtt.client as mqtt
import sqlite3
import json
from datetime import datetime, date
import time
#from cloud_mqtt import publishPL
import configparser
import paho.mqtt.publish as pub

config = configparser.ConfigParser()
config.read('config.ini')
topic = config.get('MQTT', 'topic')
broker_addr2= config.get('MQTT', 'broker_address2')
# SQLite database file path
DB_FILE = 'mqtt_2.db'

# Create a connection to the SQLite database
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS mqtt_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        DeviceName TEXT,
        Rule TEXT,
        Count INTEGER,
        Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   )
''')
conn.commit()

# Global variable to store device name
device_name = None

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #clear_retained_messages(client)
    client.subscribe("#")
    
    
    
def clear_retained_messages(client):
    # Define your topic here

    # Publish empty payloads with retain flag to clear retained messages
    client.publish((topic)+"/onvif-ej/RuleEngine/CountAggregation/Counter/&1/Out", payload=None, retain=True)
    client.publish((topic)+"/onvif-ej/RuleEngine/CountAggregation/Counter/&1/IN", payload=None, retain=True)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
   # client.publish("message-poc.spxg.net", payload="")
   # print("Message published to endpoint successfully.")
    global device_name, count_in, count_out, last_processed_date  # Access the global variables
    try:
        # Check if the topic matches the desired pattern
        if "/onvif-ej/RuleEngine/CountAggregation/Counter/&1" in msg.topic:

            # Extract device name from the topic
            device_name = msg.topic.split("/")[0]

            # Parse the JSON payload
            payload_data = json.loads(msg.payload.decode())
            # Extract rule from topic
            rule = msg.topic.split("/")[-1]

            # Determine which count variable to increment based on the rule
            count = payload_data["Data"]["Count"]
            
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Insert data into the database
            cursor.execute('''
                INSERT INTO mqtt_data (DeviceName, Rule, Count, Time)
                VALUES (?, ?, ?, ?)
            ''', (device_name, rule, count, current_time))
            conn.commit()
            # Print the received message details
            '''
            pub.single(topic=msg.topic,payload=msg.payload,qos=0,retain=True,hostname=broker_addr2,port=1883,\
                     client_id="", keepalive=60, will=None, \
                     auth=None, tls=None,\
                     protocol=mqtt.MQTTv311, transport="tcp")
            '''
            #publishPL(msg.topic,msg.payload.decode(),False)

            print(f'DeviceName: {device_name}, Rule: "{rule}", Count: {count}, Time: "{current_time}"')
            print("Data inserted into the database successfully.")
            
    except sqlite3.Error as e:
        print("Error occurred while inserting data into the database:", e)


# MQTT Broker settings
broker_address = config.get('MQTT', 'broker_address')
print(broker_address)
 # Specify the hostname or IP address of the MQTT broker
broker_port = 1883  # Specify the port number of the MQTT broker
#boker-add2=poccawd
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
# Connect to MQTT Broker
client.connect((broker_address), 1883, 60)

# Start the MQTT loop
client.loop_forever()
