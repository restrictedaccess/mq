#!/usr/bin/env python
"""
This consumer executes celery task
"""
#   2013-10-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import pika
import logging
from datetime import datetime
import simplejson as json

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.queue_declare(queue='celery_send_task', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

def execute_celery_task(ch, method, properties, body):
    """
    expects a json data
    body = dict(
        task = 'activity_tracker_notes.email_notes',
        args = []
    )
    """
    logging.info(" [x] received %s" % (body, ))
    data = json.loads(body)
    task = data['task']
    args = data['args']
    logging.info(" [x] task %s" % (task, ))
    logging.info(" [x] args %s" % (args, ))
    send_task(task, args=args)


channel.basic_consume(execute_celery_task, queue='celery_send_task', no_ack=True)
channel.start_consuming()
