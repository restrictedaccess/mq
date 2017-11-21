#!/usr/bin/env python
#   2013-05-13  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -  initial commit


import settings
import pika
import logging
from datetime import datetime
import json
from SMSUpdater import SMSUpdater

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='sms_candidate_sent_notification', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] updating %r" % (body, ))
    response = body
    body = json.loads(body)
    
    if body["mode"]=="sent":
        SMSUpdater.updateSMS(body)
    elif body["mode"]=="reply":
        SMSUpdater.receiveReplySMS(body)
    elif body["mode"]=="send":
        SMSUpdater.receiveSendSMS(body)
    else:
        SMSUpdater.send_email_alert("Failed to perform callback, invalid response data received", "Failed to perform callback, invalid response data received:<br/>{body}".format(body=response))
        

channel.basic_consume(callback, queue='sms_candidate_sent_notification', no_ack=True)
channel.start_consuming()
