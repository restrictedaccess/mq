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





logging.info('Started Invoice Checker script')

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
    
    
    result = db.view("invoice/list_invoice")
    for row in result.rows:
        
        logging.info("Changes on client docs %s" % row.id)
        c = pycurl.Curl()
        c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-invoice/')
                    
        post_data = {'couch_id': row.id}
        # Form data must be provided already urlencoded.
        postfields = urlencode(post_data)
        # Sets request method to POST,
        # Content-Type header to application/x-www-form-urlencoded
        # and data to send in request body.
        c.setopt(c.POSTFIELDS, postfields)
        
        c.perform()
        c.close()



        # Added By Josef Balisalisa for https://remotestaff.atlassian.net/browse/ER-101
        c = pycurl.Curl()

        post_data = {'isBatch': True, 'couch_id': row.id}
        postfields = urlencode(post_data)
        node_url = (settings.NODEJS_URL + "/xero/sync-invoices/?" + postfields)
        logging.info("Calling Xero Syncer %s" % node_url)
        c.setopt(c.URL, node_url)

        c.perform()
        c.close()


    db_info = db.info()
    for data in db.changes(filter='invoice/not_mongo_synced', feed='continuous', heartbeat=1000, since=db_info['update_seq']):
            
        logging.info("Changes on client docs %s" % data['id'])
        c = pycurl.Curl()
        c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-invoice/')
                    
        post_data = {'couch_id': data['id']}
        # Form data must be provided already urlencoded.
        postfields = urlencode(post_data)
        # Sets request method to POST,
        # Content-Type header to application/x-www-form-urlencoded
        # and data to send in request body.
        c.setopt(c.POSTFIELDS, postfields)
        
        c.perform()
        c.close()



        # Added By Josef Balisalisa for https://remotestaff.atlassian.net/browse/ER-101
        c = pycurl.Curl()

        post_data = {'isBatch': True, 'couch_id': data['id']}
        postfields = urlencode(post_data)
        node_url = (settings.NODEJS_URL + "/xero/sync-invoices/?" + postfields)
        logging.info("Calling Xero Syncer %s" % node_url)
        c.setopt(c.URL, node_url)

        c.perform()
        c.close()





except:
    logging.exception("Loop exited:")
    raise

logging.info('Script died.')
