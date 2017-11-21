#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   waits for changes from mailbox couchdb changes
#   -   sends the task via celery if change is found
#   -   script must be run under supervisor so that it can be restarted

from celery.execute import send_task
from celery import Celery
import sc_mailbox_celeryconfig

import couchdb
import settings
import os

os.environ['TZ'] = 'Asia/Manila'    #used for proper logging

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info('Started script')
celery = Celery()
celery.config_from_object(sc_mailbox_celeryconfig)             
            
s = couchdb.Server(settings.MAILBOX_DSN)
db = s['mailbox']

r = db.view('mailbox/unsent', startkey=[2015,11,24,0,0,0], endkey=[2020,11,24,23,59,59],descending=False)
if len(r.rows)!=0:
   for row in r.rows:
      celery.send_task('mailbox.send', [row.id])                                                                                                                                                                                           
      logging.info("sent : %s", [row.id])

try:
    db_info = db.info()
    for data in db.changes(filter='mail/send', feed='continuous', heartbeat=1000,
                           since=db_info['update_seq']):
        if settings.DEBUG:
            send_task('mailbox.send', [data['id']])
        else:    
            celery.send_task('mailbox.send', [data['id']])
            
        logging.info('sent => %s' %  data['id'])
except:
    logging.exception("Loop exited:")
    raise

logging.info('Script died.')