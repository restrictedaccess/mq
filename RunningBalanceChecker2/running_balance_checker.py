#!/usr/bin/env python
#   2015-04-26  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -   waits for changes from couchdb client docs changes
#   -   script must be run under supervisor so that it can be restarted

from celery.execute import send_task
import couchdb
import settings
import os

os.environ['TZ'] = 'Asia/Manila'    #used for proper logging

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

from pymongo import MongoClient

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





logging.info('Started Running Balance Checker script')

s = couchdb.Server(settings.COUCH_DB_DSN)
db = s['client_docs']

try:
    import pycurl
    try:
        # python 3
        from urllib.parse import urlencode
    except ImportError:
        # python 2
        from urllib import urlencode
    
    
    while True:
        result = db.view("running_balance/list_running_balance", descending=True, limit=1000)
        for row in result.rows:
            
            logging.info("Changes on client docs %s" % row.id)
            c = pycurl.Curl()
            
            c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-running-balance/')
                        
            post_data = {'couch_id': row.id}
            # Form data must be provided already urlencoded.
            postfields = urlencode(post_data)
            # Sets request method to POST,
            # Content-Type header to application/x-www-form-urlencoded
            # and data to send in request body.
            c.setopt(c.POSTFIELDS, postfields)
            
            c.perform()
            c.close()
            
            
            #update to couchdb that the document is synced to mongo
            i = 0
            while True:
                i+=1
                try:
                    logging.info("Updating document on client docs %s" % row.id)
                    doc = db.get(row.id)     
                    doc["mongo_synced"] = True
                    db.save(doc)
                    break
                except:
                    if i==10:
                        break
                    pass
            
            
except:
    logging.exception("Loop exited:")
    raise

logging.info('Script died.')
