#!/usr/bin/env python
#   2013-12-14  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack
#   -   this consumer is to be run on iweb10 server

import pika
import simplejson as json
import settings
from process_sid import process_sid

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.queue_declare(queue=settings.QUEUE, durable=True)


def callback(ch, method, properties, data):
    json_data = json.loads(data)
    for data in json_data:
        sid = data['sid']
        tz = data['tz']
        process_sid(sid, tz)


channel.basic_consume(callback, queue=settings.QUEUE, no_ack=True)
channel.start_consuming()

