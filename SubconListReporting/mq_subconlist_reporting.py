#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-10-18  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import pika
import re
import logging
from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='subconlist_reporting', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] processing %r" % (body, ))
    send_task("subconlist_reporting.process_doc_id", [body,])

channel.basic_consume(callback, queue='subconlist_reporting', no_ack=True)

channel.start_consuming()
