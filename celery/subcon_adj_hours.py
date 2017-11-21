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
from decimal import Decimal

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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
def get_adj_hrs(conn, sid, start_date, end_date):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
    
    
    total_hrs = Decimal('0.00')
    x = start_date
    start_date_ts = date(x.year, x.month, 1)

    #get all involved timesheet
    sql = text("""
        SELECT id, month_year FROM timesheet
        WHERE status IN ('open', 'locked')
        AND subcontractors_id = :sid
        AND month_year BETWEEN :start_date AND :end_date
    """)
    timesheets = conn.execute(sql, sid=sid, start_date=start_date_ts, end_date=end_date).fetchall()

    for ts in timesheets:
        sql = text("""
            SELECT day, adj_hrs FROM timesheet_details
            WHERE timesheet_id = :tid
            ORDER BY day
        """)
        ts_details = conn.execute(sql, tid=ts.id).fetchall()

        for ts_detail in ts_details:
            x = ts.month_year
            y = datetime(x.year, x.month, ts_detail.day)
            if (y >= start_date) and (y <= end_date):
                if ts_detail.adj_hrs == None:
                    continue
                adj_hrs = Decimal('%0.2f' % ts_detail.adj_hrs)
                total_hrs += adj_hrs


    return "%0.2f" % total_hrs
        
@task(ignore_result=True)
def process_doc_id(doc_id):
    logging.info('subcon_adj_hours.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
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
        generated_by = 'celery subcon_adj_hours.process_doc_id',
        html = None,
        text = 'executing subcon_adj_hours.process_doc_id %s' % doc_id,        
        subject = 'executing subcon_adj_hours.process_doc_id %s' % doc_id,
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

    
    subcon_adj_hrs_result={}
    conn = engine.connect()      
    for sid in subcontractor_ids:        
        dates=[]
        registered_hrs=[]                  
        for d in DATE_SEARCH:
            d = datetime.strptime(d, '%Y-%m-%d')         
            adj_hrs = 0.00         
            if sid:        
                adj_hrs = get_adj_hrs(conn, sid, d, d)            
                dates.append(dict(
                    date = d.strftime('%Y-%m-%d'),
                    adj_hrs = adj_hrs,                
                    )
                )   
            
                             
                        
        subcon_adj_hrs_result[int(sid)] = dates        
        
    doc['subcon_adj_hrs_result'] = subcon_adj_hrs_result    
    conn.close()    
    db.save(doc)

    #send_task("subcon_log_hours.process_doc_id", [doc_id])
    if settings.DEBUG:
        send_task("subcon_log_hours.process_doc_id", [doc_id])
    else:
        celery = Celery()
        celery.config_from_object(sc_celeryconfig)
        celery.send_task("subcon_log_hours.process_doc_id", [doc_id])    
    
        
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process_doc_id('bc7205ad9e81255ebe9b4b7496003c7b'))
