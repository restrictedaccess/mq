#!/usr/bin/env python
#   2013-06-19  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add task to send message directly to each devs 
#   2013-04-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   instead of publishing direct to office mq, it is now published to production server mq 
#   -   created an SkypeNotificationsFromProd consumer that would consume and publish to office mq
#   -   added time_limit
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added socket_timeout and connection_attempts
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
from celery.task import task
import settings
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import pika
import simplejson as json


@task(ignore_result=True, time_limit=20)
def notify_each_devs(message):
    cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
        settings.PIKA_CRED['PASSWORD'])
    params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
        virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    if settings.DEBUG == True:
        message = 'TEST %s' % message
            
    for skype_id in settings.DEVS_SKYPE_IDS:
        msg = dict(message=message, skype_id=skype_id)

        content = json.dumps(msg)
        logging.info('%r' % content)

        channel.queue_declare(queue='skype_messages', durable=True)

        channel.basic_publish(exchange='', routing_key='skype_messages', body=content)

    connection.close()


@task(ignore_result=True, time_limit=20)
def notify_devs(message):
    send_message(message, group_chat='#locsunglao/$74ee209df36c0adc')


@task(ignore_result=True, time_limit=20)
def notify_skype_id(message, skype_id):
    send_message(message, skype_id=skype_id)


def send_message(message, skype_id=None, group_chat=None):
    """
    >>> send_message('ping', skype_id='locsunglao')
    """
    if settings.DEBUG == True:
        message = 'TEST %s' % message
        
    msg = dict(message=message)
    if skype_id == None:
        msg['group_chat'] = group_chat
    else:
        msg['skype_id'] = skype_id

    content = json.dumps(msg)
    logging.info('%r' % content)

    cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
        settings.PIKA_CRED['PASSWORD'])
    params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
        virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='skype_messages', durable=True)

    channel.basic_publish(exchange='', routing_key='skype_messages', body=content)
    connection.close()


def run_tests():
    """
    >>> notify_devs('ping')
    >>> notify_skype_id('Hello World', 'locsunglao')
    >>> notify_each_devs('pong')
    """


if __name__ == "__main__":
    import doctest
    doctest.testmod()

