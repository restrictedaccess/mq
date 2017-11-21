from celery.task import task, Task
from celery.task.sets import TaskSet
from celery.execute import send_task
import couchdb
from decimal import Decimal

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone
import calendar

from sqlalchemy import create_engine
from sqlalchemy.sql import text

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import settings 
from pprint import pprint

import csv

engine = create_engine(settings.MYSQL_DSN)
conn = engine.connect()

couch_server = couchdb.Server(settings.COUCH_DSN)
db_rssc = couch_server['rssc']
db_rssc_time_records = couch_server['rssc_time_records']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')


def get_ph_time():
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


def get_timesheet(timesheet_id):
    new_cutoff_date_string = '2012-11-20'

    now = get_ph_time()
    first_day = date(now.year, now.month, 1)

    a = datetime.strptime(new_cutoff_date_string, '%Y-%m-%d')
    new_cutoff_date = date(a.year, a.month, a.day)
    sql = text("""SELECT t.*, s.work_status AS work_status, 
        s.client_price AS client_price, s.currency AS currency,
        s.job_designation as job_designation,
        l.apply_gst AS apply_gst,
        p.fname AS fname, p.lname AS lname
        FROM timesheet AS t
        JOIN subcontractors AS s
        ON t.subcontractors_id = s.id
        JOIN personal as p
        ON t.userid = p.userid
        JOIN leads as l
        ON t.leads_id = l.id
        where t.month_year >= :first_day
        AND t.month_year <= :new_cutoff_date
        AND s.prepaid = 'yes'
        AND t.id = :timesheet_id
        ORDER BY p.fname, t.month_year
        """)

    timesheet = conn.execute(sql, first_day=first_day.strftime('%F %H:%M:%S'), new_cutoff_date=new_cutoff_date.strftime('%F %H:%M:%S'), timesheet_id=timesheet_id).fetchone()
    return timesheet


def get_ts(timesheet_id):
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_client_settings = couch_server['client_docs']
    timesheet = get_timesheet(timesheet_id)

    #get client settings
    now = get_ph_time()
    now_array = [now.year, now.month, now.day, now.minute, now.second, 0]
    r = db_client_settings.view('client/settings', startkey=[timesheet.leads_id, now_array],
        endkey=[timesheet.leads_id, [2011,1,1,0,0,0,0]],
        descending=True, limit=1)

    if len(r.rows) == 0:
        currency_couch = 'Not Found'
        apply_gst_couch = 'N'
    else:
        currency_couch, apply_gst_couch = r.rows[0]['value']

    result = send_task("PrepaidGenerateClientAdjustments.GetHours",(timesheet, '2012-11-20', currency_couch, apply_gst_couch))
    data = result.get()
    return data

if __name__ == '__main__':
    f = open('adjustment_check.csv', 'wb')
    csv_writer = csv.writer(f)
    csv_writer.writerow(['client_id', 'Client Firstname', 'Client Lastname', 'Client Email', 'Currency', 'timesheet_id', 'hourly rate',
        'adjusted term', 'adjusted total_hours_worked', 'adjusted total_adj_hours', 'adjusted diff_hrs', 'adjusted diff_hrs_amount', 'adjusted particular',
        'correct term', 'correct total_hours_worked', 'correct total_adj_hours', 'correct diff_hrs', 'correct diff_hrs_amount', 'correct particular'])
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs_copy']
    view = db.view('tmp/adjusted_docs', startkey=[2012,11,23,0,0,0], endkey=[2012,11,24,0,0,0], include_docs=True)
    for r in view:
        doc = r.doc
        timesheet_id = doc['reference_data']['timesheet_id']
        from_ts = get_ts(timesheet_id)
        from_adj = doc['reference_data']
        if from_ts == None:
            logging.info(doc['_id'])
            continue
        if (from_ts['term'] != from_adj['term']) or (from_ts['diff_hours'] != from_adj['diff_hours']):
            sql = text("""
                SELECT id, fname, lname, email
                FROM leads
                where id=:leads_id
                """)
            result = conn.execute(sql, leads_id=doc['client_id'])
            lead = result.fetchone()
            csv_writer.writerow([lead.id, lead.fname, lead.lname, lead.email, doc['currency'], from_adj['timesheet_id'], from_adj['staff_hourly_rate'],
                from_adj['term'], from_adj['total_hours_worked'], from_adj['total_adj_hours'], from_adj['diff_hours'], from_adj['diff_hours_amt'], from_adj['particular'],
                from_ts['term'], from_ts['total_hours_worked'], from_ts['total_adj_hours'], from_ts['diff_hours'], from_ts['diff_hours_amt'], from_ts['particular']
                ])
            
