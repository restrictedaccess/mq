#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-11-30  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   set to execute scheduled activation as soon as possible
#   2012-04-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated mq queue

import settings
import pika
from ScheduleActivation import SubcontractorsTemp
import logging
from datetime import datetime

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='schedule_prepaid_contract', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] scheduling %r" % (body, ))
    subcontractors_temp = SubcontractorsTemp()
    """
    eta = subcontractors_temp.get_execution_date(body)
    if eta == None:
        logging.info("Failed to schedule %s." % body)
        return
    """
    eta = datetime(2012,1,1,0,0,0)
    logging.info(' [x] sending task %s @ %s' % (body, eta))
    send_task('ScheduleActivation.ScheduleActivation', args=[body,], eta=eta)

channel.basic_consume(callback, queue='schedule_prepaid_contract', no_ack=True)
channel.start_consuming()
