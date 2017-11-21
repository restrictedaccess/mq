#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2012-10-24 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix retrieval of timesheet
#   2012-10-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

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


from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from decimal import Decimal, ROUND_HALF_UP
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
def get_total_adj_hrs(sid, start_date, end_date):
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
    conn = engine.connect()
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

    conn.close()
    return "%0.2f" % total_hrs

@task	
def get_total_log_hrs(sid, start_date, end_date):
    now = get_ph_time()	
    total_work_hours = Decimal('0.00')
	
    #get all involved timesheet
    sql = text("""SELECT userid FROM subcontractors WHERE id = :sid """)
    conn = engine.connect()
    subcon = conn.execute(sql, sid=sid).fetchone()
	
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']	
	
    userid = int(int(subcon.userid))				
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

						
    conn.close()
    return "%0.2f" % total_work_hours
	
@task(ignore_result=True)
def process_doc_id(doc_id):
    logging.info('checking %s' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)

    start_date = datetime.strptime(doc['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(doc['end_date'], '%Y-%m-%d')
    subcontractor_ids = doc['subcontractor_ids']

    tasks = []
    another_tasks= []	
    for sid in subcontractor_ids:
        t = get_total_adj_hrs.subtask((sid, start_date, end_date))
        #t = get_total_adj_hrs(sid, start_date, end_date)
        tasks.append(t)
		
        r = get_total_log_hrs.subtask((sid, start_date, end_date))
        #r = get_total_log_hrs(sid, start_date, end_date)		
        another_tasks.append(r) 		

    #adj_hrs
    job = TaskSet(tasks = tasks)
    result = job.apply_async()
    data = result.join()

    couch_result = {}
    for i in range(len(subcontractor_ids)):
        couch_result[int(subcontractor_ids[i])] = data[i]

    #total log hours
    another_job = TaskSet(tasks = another_tasks)
    another_result = another_job.apply_async()
    another_data = another_result.join()

    another_couch_result = {}
    for i in range(len(subcontractor_ids)):
        another_couch_result[int(subcontractor_ids[i])] = another_data[i]

	
		
    doc = db.get(doc_id)
    doc['result'] = couch_result
    doc['total_log_hours'] = another_couch_result	
    doc['result_date_time'] = get_ph_time().strftime('%Y-%m-%d %H:%M:%S')
    db.save(doc)


if __name__ == '__main__':
    logging.info('tests')
    #logging.info(process_doc_id('4ccf6b0ffd410460e0489d66d1000f17'))
    send_task("subconlist_reporting.process_doc_id", ["4ccf6b0ffd410460e0489d66d1000f17"])
