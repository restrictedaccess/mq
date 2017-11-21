#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
import settings
import pika
from ScheduleActivation import SubcontractorsTemp
import logging

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='/', type='direct', durable=True)
channel.queue_declare(queue='activate_prepaid_contract', durable=True)

channel.basic_publish(exchange='', routing_key='activate_prepaid_contract', body='872')
logging.info(' [x] Sent data')
connection.close()

