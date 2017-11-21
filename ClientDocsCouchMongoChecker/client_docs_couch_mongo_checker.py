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



logging.info('Started Client Docs Syncer script')

s = couchdb.Server(settings.COUCH_DB_DSN)
db = s['client_docs']

try:
    for data in db.changes(filter="orders/all_orders_except_cancelled", feed='continuous', heartbeat=1000):
        logging.info("Changes on client docs %s" % data["id"])
        if settings.DEBUG:
            mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)
        else:
            mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017)
        
        
        #connect to the test db
        mongodb = mongo_client.collection_reports
        #retrieve the person collection
        col = mongodb.client_docs_order_sync
        #creating a document - CREATE        
        
        couch_doc = db.get(data["id"])
        
        
        col.insert( {"requested_on": '%s' % now}, {"couch_doc_id": data["id"]} )
            
except:
    logging.exception("Loop exited:")
    raise

logging.info('Script died.')
