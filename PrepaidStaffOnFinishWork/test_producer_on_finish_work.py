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
##~channel.exchange_declare(exchange='/', type='direct', durable=True)
channel.queue_declare(queue='prepaid_on_finish_work', durable=True)

##~channel.basic_publish(exchange='', routing_key='prepaid_on_finish_work', body='a5d3f66a6b11889e12136609c1848e86')
channel.basic_publish(exchange='', routing_key='prepaid_on_finish_work', body='0811b434cf496b852498bb201c653453')   #test daniel cowart
logging.info(' [x] Sent data')
connection.close()

