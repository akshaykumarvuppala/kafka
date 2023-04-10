from confluent_kafka import Consumer, KafkaError
from uuid import uuid4
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List
import datetime
import random
import time
import string
import mysql.connector
import pandas as pd
import json

API_KEY = 'PRSLCZ7THJJEYOHE'
API_SECRET_KEY = 'CeliVacOoZkCwjeIrzd6CnrZo1uWenVM6AT/xrVrIo4dzuswMIAmZmVi95vQiAww'
BOOTSTRAP_SERVER = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'BB5WOVLPJZRQ42FI'
SCHEMA_REGISTRY_API_SECRET = 'RMAZfu9LzV9fvNFxySg98z4TSsSWIb6sEItsWQCVdO/h/R4OcaUmeCsZot1jqmIm'
ENDPOINT_SCHEMA_URL  = 'https://psrc-knmx2.australia-southeast1.gcp.confluent.cloud'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'group.id': 'group1',
                'auto.offset.reset': "earliest",
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

mysql_host = 'localhost'  
mysql_user = 'root' 
mysql_password = 'Vvakshay1@$'  
mysql_database = 'akshay'  

cnx = mysql.connector.connect(
host=mysql_host,
user=mysql_user,
password=mysql_password,
database=mysql_database)
cursor=cnx.cursor()


consumer = Consumer(sasl_conf())
topic = 'people_details_new' 
consumer.subscribe([topic])

# Consume data from Kafka topic
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
            break
        else:
            print('Error while polling for messages: {}'.format(msg.error()))
            break
    else:
        # Process the received message
        key = msg.key()
        print(key)
        value = json.loads(msg.value())
        print(value)
        # Perform data transformation
        transformed_data = {
            'id': value['id'],
            'name': value['name'].upper(),  # Example transformation: convert name to uppercase
            'age': value['age'],  # Example transformation: increment age by 1
            'country': value['country'],
            'created_at':value['created_at']
        }
        if transformed_data['age']>=44:
            
            
        # Write transformed data to  table
            cursor.execute(f"insert into yourtable (id,name,age,country,created_at)"
                          f" values({transformed_data['id']},'{transformed_data['name']}',{transformed_data['age']},'{transformed_data['country']}','{transformed_data['created_at']}')")
            cnx.commit()
            print('Transformed data written to table: {}'.format(transformed_data))

        else:
            continue
