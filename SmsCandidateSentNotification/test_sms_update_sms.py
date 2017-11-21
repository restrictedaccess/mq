#!/usr/bin/env python
#   2013-05-13  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -  initial commit

import settings
import pika
import logging
import json

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='/', type='direct', durable=True)
channel.queue_declare(queue='sms_candidate_sent_notification', durable=True)

body = json.dumps(dict(sms_id='1', mode='sent'))

channel.basic_publish(exchange='', routing_key='sms_candidate_sent_notification', body=body)
logging.info(' [x] Sent data')
connection.close()