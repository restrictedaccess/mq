#!/usr/bin/env python
#   2015-01-26  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   initial commit
#   -   nothing seroius just sending a task
#   -   suggested daily schedule execution between 1am to 3am.



from celery.task import task, Task
from celery.execute import send_task

from celery import Celery
import sc_celeryconfig

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import settings

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone


    
def get_ph_time(as_array=False):
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    if as_array:
        return [now.year, now.month, now.day, now.hour, now.minute, now.second]
    else:
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
               
    
@task(ignore_result=True)
def process():
    #logging.info('sending task to sync_leave_request_summary.process %s' % get_ph_time())
    
    if settings.DEBUG:
        send_task("sync_leave_request_summary.process", [])
    else:
        celery = Celery()
        celery.config_from_object(sc_celeryconfig)
        celery.send_task("sync_leave_request_summary.process", [])    
				
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process())