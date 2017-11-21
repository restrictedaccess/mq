#!/usr/bin/env python
import settings
import pika
import logging

from celery.execute import send_task

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='wireless_violators', type='fanout')

channel.basic_publish(exchange='wireless_violators', routing_key='', body='{"violation": "TEST GSM 3G / Sun Cellular connection", "userid": 37950, "port": 43259, "lname": "Reynon", "host": "180.194.165.78", "fname": "Allen", "skype_id": "rs.allen.wd.ik", "email": "remote.allenreynon6@gmail.com"}') 
logging.info(' [x] Sent data')
connection.close()

