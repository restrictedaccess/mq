#!/usr/bin/env python
"""
This consumer sets the days_before_suspension to 0
"""
#   2013-08-06  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import pika
from datetime import datetime
import simplejson as json
from pytz import timezone
from pprint import pformat

import couchdb
from celery.execute import send_task

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.queue_declare(queue='set_client_days_b4_suspension', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def get_phil_time(as_array = False):
    utc_tz = timezone('UTC')
    ph_tz = timezone('Asia/Manila')
    now = utc_tz.localize(datetime.utcnow()).astimezone(ph_tz)

    #set date without timezone
    if as_array:
        now = [now.year, now.month, now.day, 
            now.hour, now.minute, now.second, now.microsecond]
    else:
        now = datetime(now.year, now.month, now.day, 
            now.hour, now.minute, now.second, now.microsecond)
    return now


def set_client_days_b4_suspension(ch, method, properties, body):
    """
    expecting a json data
    body = dict(
        leads_id = 11,
        publisher = '/path/to/script/that/published/this.php'
    )
    """
    logging.info("%s" % (body, ))
    send_task('notify_devs.send', ['set_client_days_b4_suspension', '%s' % (pformat(body, 4)), True])
    data = json.loads(body)
    leads_id = data['leads_id']

    #check client settings
    t = send_task('ClientsWithPrepaidAccounts.get_client_details', [leads_id])
    client_settings = t.get()
    doc_id = client_settings.get('doc_client_settings_id')

    if doc_id == None:
        send_task('notify_devs.send', ['mq_set_client_days_b4_suspension error', 'Please check leads_id %s, no couchdb settings found' % leads_id, True])
    else:
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['client_docs']
        doc = db.get(doc_id)
        if doc == None:
            send_task('notify_devs.send', ['mq_set_client_days_b4_suspension error', 'Please check leads_id %s, no couchdb document found for %s' % (leads_id, doc_id), True])
        else:   #update settings
            old_days_before_suspension = doc.get('days_before_suspension')
            doc['days_before_suspension'] = 0

            if doc.has_key('history'):
                history = doc['history']
            else:
                history = []

            history.append('%s mq_set_client_days_b4_suspension updated days_before_suspension from %s to %s' % (get_phil_time().strftime('%F %H:%M:%S'), old_days_before_suspension, doc['days_before_suspension']))

            doc['history'] = history

            db.save(doc)

channel.basic_consume(set_client_days_b4_suspension, queue='set_client_days_b4_suspension', no_ack=True)
channel.start_consuming()
