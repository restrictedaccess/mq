#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level

import settings
import pika
import logging
import simplejson as json

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='/', type='direct', durable=True)
channel.queue_declare(queue='schedule_salary_update', durable=True)

data = dict(
    subcon_id = 888,
    scheduled_date = '2013-03-04 20:03:00',
    timezone = 'Australia/Sydney',
    admin = 'Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>'
)

json_data = json.dumps(data)
channel.basic_publish(exchange='', routing_key='schedule_salary_update', body=json_data)
logging.info(' [x] Sent data')
connection.close()

