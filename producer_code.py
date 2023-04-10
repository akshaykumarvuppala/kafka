import argparse
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
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
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

columns=['id', 'name', 'age','country','created_at']


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
# Connect to MySQL

class People:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_people(data:dict,ctx):
        return People(record=data)

    def __str__(self):
        return f"{self.record}"

def mysql_connec(mysql_host,mysql_user,mysql_password,mysql_database,table):
    cnx = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = cnx.cursor()
    query=f'select * from {table}'
    cursor.execute(query)
    records = cursor.fetchall()
    for record in records:
        # Convert MySQL record to a dictionary
        record_dict = {
            'id': record[0],
            'name': record[1],
            'age': record[2],
         'country': record[3],
            'created_at': record[4].isoformat()
        }

        yield record_dict
        


def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

mysqll=mysql_connec(mysql_host,mysql_user,mysql_password,mysql_database,'mytable')

def my_to_dict(people:People, ctx):
    # Custom logic to serialize `obj` to a dictionary
    # using `ctx` if needed
    return people.record

query='select * from mytable'


print(mysqll)
producer = Producer(sasl_conf())
schema_registry_conf = schema_config()
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
subject = 'people_details_new'+'-value'

schema = schema_registry_client.get_latest_version(subject)
print(schema)
schema_str=schema.schema.schema_str
print(schema_str)

string_serializer = StringSerializer('utf_8')
print(string_serializer)



producer.poll(0.0)
try:
    for record_dict in mysql_connec(mysql_host,mysql_user,mysql_password,mysql_database,'mytable'):
        print(record_dict)
        producer.produce(topic='people_details_new',
                            key=string_serializer(str(uuid4())),
                            value=json.dumps(record_dict),
                            on_delivery=delivery_report)
        
except KeyboardInterrupt:
        pass
except ValueError:
        print("Invalid input, discarding record...")
        pass

print("\nFlushing records...")
producer.flush()

