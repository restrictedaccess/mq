#!/usr/bin/env python
#   2013-05-13  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -  initial commit


import settings
import pika
import logging
from datetime import datetime
import json
from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='receive_job_order_query', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] scheduling %r" % (body, ))
    response = body
    body = json.loads(body)
    query = body["query"]
    eta = datetime(2012,1,1,0,0,0)
    logging.info(' [x] sending task %s @ %s' % (query, eta))
    send_task("job_order_sync.job_order_sync", args=[query], eta=eta)
        

channel.basic_consume(callback, queue='receive_job_order_query', no_ack=True)
channel.start_consuming()