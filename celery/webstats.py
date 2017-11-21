#   2014-01-06  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added parameter to store results via shelve
#   -   note that there are some unclosed couchdb timerecords doc resulting to big number of hours worked
#   2013-09-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated totals to reduce records to compute
#   2013-04-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added time_limit
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2012-10-19 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   seperate the task active_contracts to be called less frequently
#   2012-10-18 Norman
#   -   added randomisation of subcontracts
#   2012-10-16 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added total_active_contracts
#   2012-10-15 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task for updating webstats

import settings
import couchdb
import redis
from celery.task import task
from datetime import datetime, timedelta
from pytz import timezone

from persistent_mysql_connection import engine
from sqlalchemy.sql import text
import shelve

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import random
import string

STARTKEY = [2013,9,11,0,0,0]
RECORDED_HRS = 2230063.6600

@task(ignore_result=True, time_limit=20)
def update_stats(store_results=False):
    """updates the redis webstats key 
    """
    s = couchdb.Server(settings.COUCH_DSN)
    couchdb_rssc = s['rssc']
    view = couchdb_rssc.view('staff/working')
    total_working_staff = view.total_rows
    rate_per_second = round(total_working_staff / 3600.0, 4)
    logging.info('total_working_staff : %s' % total_working_staff)
    logging.info('rate_per_second : %0.4f' % rate_per_second)

    #stored data on shelve
    d = shelve.open('web_stats_temp.db')
    if d.has_key('RECORDED_HRS'):
        recorded_hrs = d['RECORDED_HRS']
    else:
        recorded_hrs = RECORDED_HRS

    if d.has_key('STARTKEY'):
        startkey = d['STARTKEY']
    else:
        startkey = STARTKEY

    #grabbed total hours from latest log as of 2013-09-11
    total_hours = timedelta(hours=recorded_hrs)

    
    couchdb_time_records = s['rssc_time_records']
    #get regular records
    view = couchdb_time_records.view('summary/in_out', startkey=startkey)
    for row in view:
        x = row['key']
        y = row['value']
        time_in = datetime(x[0], x[1], x[2], x[3], x[4], x[5])
        time_out = datetime(y[0], y[1], y[2], y[3], y[4], y[5])
        dt = time_out - time_in
        total_hours += dt

    #get lunch records
    total_lunch = timedelta(seconds=0)
    view = couchdb_time_records.view('summary/lunch_in_out', startkey=startkey)
    for row in view:
        x = row['key']
        y = row['value']
        time_in = datetime(x[0], x[1], x[2], x[3], x[4], x[5])
        time_out = datetime(y[0], y[1], y[2], y[3], y[4], y[5])
        dt = time_out - time_in
        total_lunch += dt

    #get currently working records
    total_current_working = timedelta(seconds=0)
    view = couchdb_time_records.view('summary/working', startkey=startkey)

    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow()).astimezone(phtz)
    now = [now.year, now.month, now.day, now.hour, now.minute, now.second]
    now_recorded = now
    now = datetime(now[0], now[1], now[2], now[3], now[4], now[5])
    for row in view:
        x = row['key']
        time_in = datetime(x[0], x[1], x[2], x[3], x[4], x[5])
        dt = now - time_in
        total_current_working += dt

    total_hours = total_hours - total_lunch + total_current_working
    hours = (total_hours.days * 24) + (total_hours.seconds / 3600.0)
    total_hours = round(hours, 2)
    logging.info('total_hours : %0.4f' % total_hours)

    #get total_staff_worked
    #get count 
    sql = text("""
        SELECT COUNT(*)
        FROM subcontractors
        """)
    conn = engine.connect()
    total_staff_worked = conn.execute(sql).fetchone()[0]
    logging.info('total_staff_worked : %s' % total_staff_worked)

    #get all subcon ids	
    sql = text("""
        SELECT count(id)
        FROM subcontractors
        WHERE status in ('ACTIVE', 'suspended')
        """)
    total_active_contracts = conn.execute(sql).fetchone()[0]

    logging.info('total_active_contracts : %s' % total_active_contracts)
		
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    webstats = dict(
        rate_per_second = '%0.4f' % rate_per_second,
        total_hours = "%0.4f" % total_hours,
        total_working_staff = "%s" % total_working_staff,
        total_staff_worked = "%s" % total_staff_worked,
        total_active_contracts = "%s" % total_active_contracts,
    )
    r.hmset('webstats', webstats)
    conn.close()

    if store_results == True:
        total_current_working_hrs = (total_current_working.days * 24) + (total_current_working.seconds / 3600.0)
        recorded_hrs = total_hours - total_current_working_hrs
        d['RECORDED_HRS'] = recorded_hrs
        d['STARTKEY'] = now_recorded 
        d.sync()


@task(ignore_result=True, time_limit=20)
def update_stats_active_contract():
    """updates the redis webstats active_contracts hash
    """
    #get all subcon ids	
    sql = text("""
        SELECT id
        FROM subcontractors
        WHERE status in ('ACTIVE', 'suspended')
        """)
    conn = engine.connect()
    active_contracts = conn.execute(sql).fetchall()
    conn.close()
    random.shuffle(active_contracts, random.random)	
    active_contract_ids=[]	
    for a in active_contracts:
        active_contract_ids.append('%s' % a.id)	

    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.hmset('webstats', dict(active_contracts=string.join(active_contract_ids, ',')))
    logging.info('active_contracts : %s' % active_contract_ids)


def run_tests():
    """
##~    >>> update_stats(True)

    >>> update_stats()

##~    >>> update_stats_active_contract()

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
