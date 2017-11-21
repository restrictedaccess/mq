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
try:
    import locale
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    pass

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





logging.info('Started Time Records Checker script')

s = couchdb.Server(settings.COUCH_DB_DSN)
db = s['rssc_time_records']

try:
    import pycurl
    try:
        # python 3
        from urllib.parse import urlencode
    except ImportError:
        # python 2
        from urllib import urlencode
    
    counter = 0
    result = db.view("rssc_time_records/list_rssc_time_records", limit=20000)
    for row in result.rows:

        #print row            
        #print "Changes on rssc_time_records docs %s\n" % row.id
        logging.info("Started syncing rssc_time_records docs syncer %s\n" % row.id)
        
                    
        c = pycurl.Curl()
        c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-rssc-time-records/')

        post_data = {'couch_id': row.id}
        # Form data must be provided already urlencoded.
        postfields = urlencode(post_data)
        # Sets request method to POST,
        # Content-Type header to application/x-www-form-urlencoded
        # and data to send in request body.
        c.setopt(c.POSTFIELDS, postfields)
        
        c.perform()
        c.close()
    
    db_info = db.info()
    for data in db.changes(filter='rssc_time_records/not_mongo_synced', feed='continuous', heartbeat=1000, since=db_info['update_seq']):
            
        logging.info("Changes on rssc time records docs heartbeat %s" % data['id'])
        c = pycurl.Curl()
        c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-rssc-time-records/')
                    
        post_data = {'couch_id': data['id']}
        # Form data must be provided already urlencoded.
        postfields = urlencode(post_data)
        # Sets request method to POST,
        # Content-Type header to application/x-www-form-urlencoded
        # and data to send in request body.
        c.setopt(c.POSTFIELDS, postfields)
        
        c.perform()
        c.close()
                    
            
            
            
except:
    logging.exception("Loop exited:")
    raise

logging.info('Script died.')
