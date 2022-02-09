# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 16:23:52 2022

@author: manal
"""

import json
from kafka import KafkaProducer
import time
#On se connecte à la machine Kafka
producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))#On récupère chaque ligne du fichier json dans une liste
with open('Alcohol.csv', 'r') as f:

    listusers= f.readlines()
    for i in listusers:
        user=i.strip()
        print(user)
        producer.send('quickstart-events', user)
        time.sleep(10)

