#!/usr/bin/env python
#   2014-04-04  Lawrence Oliver Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the latest active timesheet for scheduling OT

import settings
import couchdb
import re
import string
import pika
from pprint import pprint, pformat

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

from celery.task import task, Task
from celery.execute import send_task
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from calendar import monthrange
import simplejson as json

from operator import itemgetter
from decimal import Decimal, ROUND_HALF_UP

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

DAY_TO_SEND = 'Sat'
HR_TO_SEND = 0
MIN_TO_SEND = 1
TWOPLACES = Decimal(10) ** -2


@task(ignore_result=True)
def send_to_mq(sids):
    """
    sent to mq for processing it on the other server
    """
    logging.info('sending to staff_ot_email mq : %s' % sids)
    parameters = pika.URLParameters(settings.PIKA_URL_STAFF_OT_EMAIL)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue='staff_ot_email', durable=True)
    channel.basic_publish(exchange='', routing_key='staff_ot_email', body=json.dumps(sids))
    connection.close()


@task
def get_active_suspended_contracts():
    """
    scheduled Thursday mornings to collect active and suspended contracts
    and have it executed on Saturday morning
    """
    conn = engine.connect()
    sql = text("""SELECT id, client_timezone FROM subcontractors 
        WHERE status IN ('ACTIVE', 'suspended')
        """)
    subcontractors = conn.execute(sql).fetchall()

    time_collection = {}    #store keys as datetime and values as sid array to summarize
    now = datetime.now()
    ph_tz = timezone('Asia/Manila')
    for subcontractor in subcontractors:
        #get the latest active timesheet
        sql = text("""
            SELECT tz_lookup.timezone
            FROM timesheet AS t
            JOIN timezone_lookup AS tz_lookup
            ON t.timezone_id = tz_lookup.id
            WHERE t.subcontractors_id = :sid
            AND t.status != 'deleted'
            ORDER BY t.month_year
            LIMIT 1
        """)
        tz_result = conn.execute(sql, sid=subcontractor.id).fetchone()
        if tz_result == None:
            send_task('notify_devs.send', ['Please check sid:%s' % (subcontractor.id), 'No timesheet found while scheduling OT emails'])
            timesheet_timezone = timezone('Asia/Manila')    # no timesheet yet
        else:
            a = tz_result[0]
            timesheet_timezone = timezone(a)
        
        dt = datetime(now.year, now.month, now.day, HR_TO_SEND, MIN_TO_SEND, 0, tzinfo=timesheet_timezone)

        # loop til we reach desired date
        day = dt.strftime('%a')
        while dt.strftime('%a') != DAY_TO_SEND:
            dt += timedelta(days=1)

        dt_ph = dt.astimezone(ph_tz)
        logging.info('schedule staff_ot_email : %s %s %s' % (dt_ph, subcontractor.id, timesheet_timezone))
        if dt_ph not in time_collection.keys():
            time_collection[dt_ph] = [
                dict(
                    sid = subcontractor.id, 
                    tz = '%s' % timesheet_timezone
                )
            ]
        else:
            sids = time_collection[dt_ph]
            sids.append(
                dict (
                    sid = subcontractor.id, 
                    tz = '%s' % timesheet_timezone
                )
            )
            time_collection[dt_ph] = sids


    for k, v in time_collection.iteritems():
        if settings.DEBUG:  #execute immediately
            send_task('overtime.send_to_mq', args=[v])
        else:
            send_task('overtime.send_to_mq', args=[v], eta=k)

    conn.close()


def run_tests():
    """
    >>> get_active_suspended_contracts()
    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()

