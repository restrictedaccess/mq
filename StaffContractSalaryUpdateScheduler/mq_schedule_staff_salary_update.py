#!/usr/bin/env python
"""
This consumer schedules activation of salary
"""
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import pika
import logging
from datetime import datetime
import simplejson as json
from pytz import timezone

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='schedule_salary_update', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    """
    expecting a json data
    data = dict(
        subcon_id = request.POST['subcon_id'],
        scheduled_date = schedule.scheduled_date.strftime('%Y-%m-%d %H:%M:%S'),
        timezone = 'Asia/Manila',
        admin = '%s %s <%s>' % (admin.pk, admin.admin_fname, admin.admin_lname)
    )
    """
    logging.info(" [x] salary scheduling %r" % (body, ))
    send_task('skype_messaging.notify_devs', ['Schedule Salary Update : %r' % body])
    data = json.loads(body)
    subcon_id = data['subcon_id']
    eta = datetime.strptime(data['scheduled_date'], '%Y-%m-%d %H:%M:%S')
    tz = timezone(data['timezone'])
    eta = tz.localize(eta)
    ph_tz = timezone('Asia/Manila')
    eta = eta.astimezone(ph_tz)
    logging.info(' [x] sending task %s @ %s' % (subcon_id, eta))
    send_task('ScheduleActivation.StaffSalaryUpdate', args=[subcon_id,], eta=eta)

channel.basic_consume(callback, queue='schedule_salary_update', no_ack=True)
channel.start_consuming()
