#!/usr/bin/env python
#   this mq is intended to run on replication server 

#   2013-07-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import pika
import settings
import logging
import os

from pprint import pformat

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)                                                          
channel = connection.channel()
channel.queue_declare(queue='image_replication', durable=True)


def callback(ch, method, properties, data):
    logging.info('%s:%s'% (properties.headers.get("image_type"), properties.headers.get("file_name")))
    file_path = '%s/%s/%s/%s' % (settings.FILE_PATH,
        properties.headers.get('year'),
        properties.headers.get('week'),
        properties.headers.get('image_type'))

    #if not os.path.exists(file_path):
    #    os.makedirs(file_path)

    #f = open('%s/%s' % (file_path, properties.headers.get('file_name')), 'wb')
    #f.write(data)
    #f.close()


channel.basic_consume(callback, queue='image_replication', no_ack=True)
channel.start_consuming()


