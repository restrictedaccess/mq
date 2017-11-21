#!/usr/bin/env python
"""
This consumer schedules blanking of personal.mass_responder_code
"""
#   2013-05-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   disabled skype messaging
#   2013-05-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import pika
import logging
from datetime import datetime
import simplejson as json
from pytz import timezone

from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel_mass_responder_code = connection.channel()
channel_mass_responder_code.queue_declare(queue='expire_mass_responder_code', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

def schedule_mass_responder_code_expire(ch, method, properties, body):
    """
    expecting a json data
    body = dict(
        userid = 69,
        scheduled_date = '2013-05-15 09:23:00', #Asia/Manila timezone
        mass_responder_code = 'fffa69ff84bbb0ce4674cac984293bb339153f17',
        script = '/path/to/script/that/published/this.php'
    )
    """
    logging.info(" [x] mass responder code %s" % (body, ))
    data = json.loads(body)
    userid = data['userid']
    mass_responder_code = data['mass_responder_code']
    eta = datetime.strptime(data['scheduled_date'], '%Y-%m-%d %H:%M:%S')
    x = eta.timetuple()
    ph_tz = timezone('Asia/Manila')
    eta = datetime(x[0], x[1], x[2], x[3], x[4], x[5], tzinfo=ph_tz)
    logging.info(' [x] sending task %s @ %s' % (userid, eta))
    send_task('schedule.expire_mass_responder_code', args=[userid, mass_responder_code], eta=eta)


channel_mass_responder_code.basic_consume(schedule_mass_responder_code_expire, queue='expire_mass_responder_code', no_ack=True)
channel_mass_responder_code.start_consuming()
