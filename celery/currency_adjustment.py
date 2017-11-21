# 2016-03-24 Allanaire Tapion 
# Library for currency adjustment
from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet
from celery import Celery

import settings
from persistent_mysql_connection import engine
from sqlalchemy.sql import text
import couchdb
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
import redis
from pymongo import MongoClient

import locale
import logging
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

utc = timezone('UTC')
phtz = timezone('Asia/Manila')

WORKING_WEEKDAYS = 22


def get_ph_time(as_array=False):
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    if as_array == True:
        return [now.year, now.month, now.day, now.hour, now.minute, now.second]
    else:
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)

@task
def get_currency_apply_gst_client(leads_id):
    #check if client has couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']
    now = get_ph_time(as_array = True)
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    data = r.rows[0]
    doc = data.doc
    return data['value']
    couch_currency, apply_gst = data['value']
    return couch_currency
@task
def get_forex_rate_client(leads_id):
    conn = engine.connect()
    couch_currency, apply_gst = get_currency_apply_gst_client(leads_id)
    currency_sql = text("""
        SELECT rate FROM currency_adjustments WHERE active='yes' AND currency=:currency
    """)
    forex = conn.execute(currency_sql, currency = couch_currency).fetchone()
    return forex

@task
def get_currency_adjustment_per_staff(subcon_id): 
    conn = engine.connect()
    subcon = get_contract_detail(subcon_id)
    forex = get_forex_rate_client(subcon.leads_id)
    forex_rate = float(forex.rate)
    subcon_current_rate = float(subcon.current_rate)
    hourly_rate = get_hourly_rate_per_staff(subcon_id)
    return float("{0:.4f}".format((hourly_rate*(subcon_current_rate - forex_rate))/forex_rate))
@task
def get_currency_adjustment_peso_per_staff(subcon_id): 
    conn = engine.connect()
    subcon = get_contract_detail(subcon_id)
    forex = get_forex_rate_client(subcon.leads_id)
    forex_rate = float(forex.rate)
    subcon_current_rate = float(subcon.current_rate)
    return float("{0:.2f}".format((subcon_current_rate - forex_rate)))
@task
def get_contract_detail(subcon_id):
    #get subcon details
    subcon_sql = text("""
        SELECT id, leads_id, userid, current_rate, client_price, work_status FROM subcontractors WHERE id = :subcon_id
    """)
    
    conn = engine.connect()
    subcon = conn.execute(subcon_sql, subcon_id = subcon_id).fetchone()
    return subcon
@task    
def get_forex_rate_per_staff(subcon_id):
    subcon = get_contract_detail(subcon_id)
    forex = get_forex_rate_client(subcon.leads_id)
    return forex

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

@task
def save_currency_adjustment_allocation_per_staff(subcon_id,start_date, end_date,order_id, currency_difference, total):
    
    
    
    client = MongoClient(host=settings.MONGO_PROD, port=27017)
    db = client.currency_adjustments
    currency_adjustment_allocations = db.currency_adjustment_allocations
    subcon = get_contract_detail(subcon_id)
    #look for existing currency adjustment for the specified invoice
    currency_adjustment_allocation = currency_adjustment_allocations.find_one({"subcon_id":subcon_id, "order_id":order_id})
    forex = get_forex_rate_client(subcon.leads_id)
    forex_rate = float(forex.rate)
    import time
    start_date = datetime.combine(start_date, datetime.min.time())
    start_date_ts = time.mktime(start_date.timetuple())
    end_date = datetime.combine(end_date, datetime.min.time())
    end_date_ts = time.mktime(end_date.timetuple())
    
    data = dict(subcon_id=subcon_id, contract_current_rate=float("{0:.2f}".format(float(subcon.current_rate))), forex_rate=forex_rate,start_date_ts=start_date_ts,start_date=start_date,end_date_ts=end_date_ts,end_date=end_date, order_id=order_id, currency_difference=currency_difference, total=total)
    
    logging.info("Adding allocation for subcon %s" % (subcon_id))
    
    #insert transaction for proper credit/charge
    if total <> 0:
        currency_adjustment_allocations.insert(data)
    
    
@task
def get_hourly_rate_per_staff(subcon_id): 
    subcon_sql = text("""
        SELECT id, client_price, work_status FROM subcontractors WHERE id = :subcon_id
    """)
    
    conn = engine.connect()
    subcon = conn.execute(subcon_sql, subcon_id = subcon_id).fetchone()
    client_price = float(subcon.client_price)
    if subcon.work_status == "Part-Time":
        return float("{0:.2f}".format((client_price * 12)/52/5/4))
    else:
        return float("{0:.2f}".format((client_price * 12)/52/5/8))
