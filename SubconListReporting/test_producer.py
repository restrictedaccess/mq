#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
import settings
import pika
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='/', type='direct', durable=True)
channel.queue_declare(queue='subconlist_reporting', durable=True)

channel.basic_publish(exchange='', routing_key='subconlist_reporting', body='sample_document_id')
logging.info(' [x] Sent data')
connection.close()

