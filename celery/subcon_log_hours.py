#   2014-05-02  Normaneil Macutay<normanm@remotestaff.com.au>
#   - Initial Hack.  Task to get subcon login date/time in couchdb rssc_time_records

import settings
import couchdb
import re
import string
from pprint import pprint, pformat

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet
from celery import Celery
import sc_celeryconfig


from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal, ROUND_HALF_UP

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

TWOPLACES = Decimal(10) ** -2

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
        
@task    
def get_log_hrs(sid, start_date, end_date, userid):
    now = get_ph_time()    
    total_work_hours = Decimal('0.00')
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']    
    
    userid = int(int(userid))                
    r = db.view('rssc_reports/userid_timein', 
        startkey=[userid, [ int(start_date.strftime('%Y')), int(start_date.strftime('%m')), int(start_date.strftime('%d')),0,0,0,0]], 
        endkey=[userid, [ int(end_date.strftime('%Y')), int(end_date.strftime('%m')), int(end_date.strftime('%d')),23,59,59,0]],
        ascending=True        
    )
    
    phtz = timezone('Asia/Manila')    
    timezone_ref = phtz

    
    for row in r.rows:
            
        record_type, b, leads_id, subcon_id = row['value']
        userid, a = row['key']
        
        if subcon_id != None:        
            if record_type == 'quick break':
                continue
             
            if int(sid) != int(subcon_id):
                continue

            start = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=phtz)
            if b == None or b == False:
                end = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, tzinfo=phtz)
            else:
                end = datetime(b[0], b[1], b[2], b[3], b[4], b[5], tzinfo=phtz)     
        
            start = start.astimezone(timezone_ref)
            if end != None:
                end = end.astimezone(timezone_ref)

                #update totals
                time_diff = end - start
                time_diff_decimal = Decimal('%s' % (time_diff.seconds / 3600.0)).quantize(TWOPLACES, rounding=ROUND_HALF_UP)            
           
                if record_type == 'time record':
                    total_work_hours += time_diff_decimal
                elif record_type == 'lunch record':
                    total_work_hours -= time_diff_decimal        

                        

    return "%0.2f" % total_work_hours
        
@task(ignore_result=True)
def process_doc_id(doc_id):
    logging.info('subcon_log_hours.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    
    #send notice to devs
    now = get_ph_time(as_array=False) 
    to=['devs@remotestaff.com.au']
    couch_mailbox = s['mailbox']    
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]          
    mailbox = dict(
        sent = False,        
        bcc = None,
        cc = None,
        created = date_created,
        generated_by = 'celery subcon_log_hours.process_doc_id',
        html = None,
        text = 'executing subcon_log_hours.process_doc_id %s' % doc_id,        
        subject = 'executing subcon_log_hours.process_doc_id %s' % doc_id,
        to = to            
    )
    mailbox['from'] = 'noreply@remotestaff.com.au'        
    #couch_mailbox.save(mailbox)    
    
    #subconlist_reporting doc    
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
        
    subcontractor_ids = doc['subcontractor_ids']
    DATE_SEARCH = doc['date_search']

    
    subcon_log_hrs_result={}
    
    for sid in subcontractor_ids:
        userid =  doc['subcon_userid'][sid]    
        dates=[]
        registered_hrs=[]                  
        for d in DATE_SEARCH:
            d = datetime.strptime(d, '%Y-%m-%d')         
            log_hrs = 0.00         
            if sid:        
                log_hrs = get_log_hrs(sid, d, d, userid)            
                dates.append(dict(
                    date = d.strftime('%Y-%m-%d'),
                    log_hrs = log_hrs,                
                    )
                )   
            
                             
                        
        subcon_log_hrs_result[int(sid)] = dates        
        
    doc['subcon_log_hrs_result'] = subcon_log_hrs_result    
    db.save(doc)

        
    if settings.DEBUG:
        send_task("subcon_total_adj_hours.process_doc_id", [doc_id])
    else:
        celery = Celery()
        celery.config_from_object(sc_celeryconfig)
        celery.send_task("subcon_total_adj_hours.process_doc_id", [doc_id])    
        
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process_doc_id('bc7205ad9e81255ebe9b4b7496003c7b'))
