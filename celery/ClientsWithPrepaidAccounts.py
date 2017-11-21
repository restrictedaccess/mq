#! /usr/bin/env python
#   2014-08-18  Normaneil E. Macuutay <normanm@remotestaff.com.au>
#   -   used column field subcontractors.end_date instead of subcontractors.date_terminated or resignation_date
#   2014-04-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updates affected due to changes in subcontractors table
#   2013-08-06  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added doc_client_settings_id for easier retrieval of client settings doc
#   2013-08-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add last_active_contract_date for filtering client details
#   -   anticipate resignation dates with NULL values
#   2013-07-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   show all clients regardless of subcontractors.status on https://remotestaff.com.au/portal/AdminRunningBalanceClientSearch/ClientSearch.html
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used persistent connection on mysql
#   2013-01-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3505, added autodebit field
#   -   catch null company_name,company_address
#   2013-01-10  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added company_name and company_address
#   2013-01-03  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added days_before_suspension
#   2012-12-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered client settings with override wf 3376
#   2012-12-07  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   close all mysql connection
#   2012-06-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   anticipate subcontractors record recently de-activated to show client on prepaid adjustment

import string
from celery.task import task, Task
from celery.task.sets import TaskSet
import couchdb
from decimal import Decimal

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone
import calendar

from persistent_mysql_connection import engine
from sqlalchemy.sql import text
from pprint import pprint

import settings 

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


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


def map_return_dates(record):
    if record.status == 'terminated':
        if record.date_terminated == None:
            if record.end_date == None:
                return datetime(1980,1,1,0,0,0)
            return record.end_date                
        return record.date_terminated
        
        
    else:
        if record.resignation_date == None:
            if record.end_date == None:
                return datetime(1980,1,1,0,0,0)
            return record.end_date                
        return record.resignation_date


@task
def get_client_details(client_id):
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    #get running balance
    r = db.view('client/running_balance', key=int(client_id))

    if len(r.rows) == 0:
        running_balance = Decimal('0.0')
    else:
        running_balance = Decimal('%s' % r.rows[0].value)

    autodebit = 'N'

    #get currency
    now = get_ph_time()
    now = [now.year, now.month, now.day, now.hour, now.minute, now.second, 0]
    r = db.view('client/settings', startkey=[int(client_id), now], endkey=[int(client_id), [2011,1,1,0,0,0,0]], descending=True, limit=1, include_docs=True)

    days_before_suspension = 0
    if len(r.rows) == 0:
        currency = 'AUD'
        doc_client_settings = {'_id':None}
    else:
        currency = r.rows[0].value[0]
        doc_client_settings = r.rows[0].doc

        if doc_client_settings.has_key('days_before_suspension'):
            days_before_suspension = doc_client_settings['days_before_suspension']

        if doc_client_settings.has_key('autodebit'):
            autodebit = doc_client_settings['autodebit']


    conn = engine.connect()

    sql = text("""SELECT fname, lname, email, status, company_name, company_address FROM leads WHERE id=:client_id""")
    r = conn.execute(sql, client_id=client_id).fetchone()

    #get latest contract
    sql = text("""SELECT id FROM subcontractors WHERE STATUS IN ('ACTIVE', 'suspended') AND leads_id=:client_id""")
    active_contracts = conn.execute(sql, client_id=client_id).fetchall()
    if len(active_contracts) > 0:
        last_active_contract_date = get_ph_time()
    else:
        sql = text("""
            SELECT id, status, date_terminated, resignation_date, end_date
            FROM subcontractors
            WHERE status IN ('terminated', 'resigned')
            AND leads_id=:client_id
        """)
        old_contracts = conn.execute(sql, client_id=client_id).fetchall()
        if len(old_contracts) == 0:
            last_active_contract_date = None
        else:
            date_list = map(map_return_dates, old_contracts)
            date_list.sort()           
            last_active_contract_date = date_list[-1]

    conn.close()

    return dict(
        fname = string.capitalize(string.strip(r.fname)),
        lname = string.capitalize(string.strip(r.lname)),
        client_id = client_id,
        currency = currency,
        email = string.strip(r.email),
        status = string.strip(r.status),
        days_before_suspension = days_before_suspension,
        running_balance = running_balance,
        company_name = string.strip('%s' % r.company_name),
        company_address = string.strip('%s' % r.company_address),
        autodebit = autodebit,
        last_active_contract_date = last_active_contract_date,
        doc_client_settings_id = doc_client_settings.get('_id')
    )


@task
def get_clients_with_prepaid_accounts():
    sql = text("""
        SELECT DISTINCT(leads_id) FROM subcontractors
        WHERE prepaid = 'yes'
        """)
    conn = engine.connect()
    r = conn.execute(sql).fetchall()

    client_ids = []
    for x in r:
        client_ids.append(x[0])

    #retrieve leads_id of timesheet records for the past 90 days
    #this is to prevent subcontractors record recently terminated and does not show up on adjustment
    now = get_ph_time()
    prev_date = now - timedelta(days=90)

    now_string = now.strftime('%F %T')
    prev_date_string = prev_date.strftime('%F %T')

    sql = text("""
        SELECT DISTINCT(t.leads_id) 
        FROM timesheet AS t
        JOIN subcontractors AS s
        ON t.leads_id = s.leads_id
        WHERE s.prepaid = 'yes'
        AND month_year BETWEEN :prev_date_string AND :now_string
        """)
    r = conn.execute(sql, prev_date_string=prev_date_string, now_string=now_string)

    for x in r:
        client_id = x[0]
        if client_id not in client_ids:
            client_ids.append(client_id)
    
    tasks = []
    for client_id in client_ids:
        y = get_client_details.subtask((client_id,))
        tasks.append(y)

    job = TaskSet(tasks = tasks)
    result = job.apply_async()
    data = result.join()
    conn.close()
    return data


@task
def get_clients_daily_rate(leads_id):
    """given the leads_id, return the daily rate in decimal
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    #check if client has couchdb settings
    x = get_ph_time()
    now = [x.year, x.month, x.day, x.hour, x.minute, x.second, 0]
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, raise an exception
        raise Exception, "Please check leads_id : %s . No couchdb client settings found." % (leads_id)

    couch_currency, apply_gst = r.rows[0]['value']
    doc = r.rows[0].doc

    #get items
    sql = text("""
        SELECT s.id, s.client_price, s.currency,
        s.job_designation, s.work_status, 
        p.fname, p.lname
        FROM subcontractors as s
        LEFT JOIN personal AS p
        ON s.userid = p.userid
        WHERE s.leads_id = :leads_id
        AND prepaid='yes'
        AND s.status IN ('ACTIVE', 'suspended')
    """)
    conn = engine.connect()
    items = conn.execute(sql, leads_id=leads_id).fetchall()

    daily_rate = Decimal(0)
    for item in items:
        sid, client_price, currency, job_designation, work_status, fname, lname = item
        if couch_currency != currency:
            raise Exception, "Please check subcontractors.id : %s . Currency does not match with clients couch settings : %s vs %s" % (sid, couch_currency, currency)

        staff_daily_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0))

        #check client settings for override
        if doc.has_key('override_hours_per_invoice'):
            if doc['override_hours_per_invoice'].has_key('%s' % sid):
                if doc['override_hours_per_invoice']['%s' % sid].has_key('hours_per_day'):
                    override_hours_per_day = doc['override_hours_per_invoice']['%s' % sid]['hours_per_day']
                    override_hours_per_day = Decimal('%s' % override_hours_per_day)
                    if work_status == 'Part-Time':
                        hours_per_day = Decimal('4.0')
                    else:
                        hours_per_day = Decimal('8.0')

                    hourly_rate = staff_daily_rate / hours_per_day
                    staff_daily_rate = hourly_rate * override_hours_per_day

        daily_rate += staff_daily_rate

    conn.close()
    return daily_rate


@task
def get_multiple_clients_daily_rate(client_ids):
    tasks = []
    for client_id in client_ids:
        y = get_clients_daily_rate.subtask((client_id,))
        tasks.append(y)

    job = TaskSet(tasks = tasks)
    result = job.apply_async()
    result = result.join()
    return result


def run_tests():
    """
    >>> a = get_client_details(11)
    >>> logging.info(a)

    >>> a = get_client_details(2020)
    >>> logging.info(a)

    #terminated
    >>> a = get_client_details(8877)
    >>> logging.info(a)

    #resigned
    >>> a = get_client_details(7492)
    >>> logging.info(a)

    #terminated, resigned
    >>> a = get_client_details(8849)
    >>> logging.info(a)

    #terminated, resigned
    >>> a = get_client_details(6990)
    >>> logging.info(a)

    #terminated, resigned
    >>> a = get_client_details(5781)
    >>> logging.info(a)





##~    >>> logging.info(a)
    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
##~    result = send_task("ClientsWithPrepaidAccounts.get_clients_with_prepaid_accounts", [])
##~    result = send_task("ClientsWithPrepaidAccounts.get_clients_daily_rate", [6630,])
##~    data = result.get()
##~    pprint(data)
