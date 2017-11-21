#!/usr/bin/env python
#   this mq is intended to run on test and production servers for deployments

#   2013-07-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import pika
import simplejson as json
import settings
from fabric.api import lcd, local, env
from celery.execute import send_task

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.queue_declare(queue=settings.REPO_USERNAME, durable=True)


def callback(ch, method, properties, data):
    logging.info(" [x] %r" % (data, ))
    json_data = json.loads(data)
    fab_cmd = json_data['fab_cmd']
    output = local('fab %s' % fab_cmd, capture=True)
    send_task('skype_messaging.notify_devs', [output])   #TODO uncomment 
##~    send_task('skype_messaging.notify_skype_id', [output, 'locsunglao'])


channel.basic_consume(callback, queue=settings.REPO_USERNAME, no_ack=True)
channel.start_consuming()

