#!/usr/bin/env python
#   2013-12-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added rica on excemption for monitoring hourly rate
#   2013-11-14  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task for comparing hourly rates based on recorded on subcontractors vs currently being used by staff
#   2013-11-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task for checking leads_id of a subcontractor document
#   2013-09-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered blank string on subcontractors_client_rate.work_status as per norman
#   2013-08-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used ROUND_HALF_UP on rounding off hourly rate
#   2013-05-10  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task for retrieving rates, hours_per_day, client_price, hourly_rate
#   2013-05-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added conn.close()
#   2013-04-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bug fix on noreply email
#   -   ignored rate changes that has end_dates not NULL as per norman
#   2013-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3807 restrict auto creation of invoice when there is a hourly rate change
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix test result for subcontract 2096
#   2013-02-07  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   on retrieving hourly rates, disregard all recorded on couchdb before 2013-02-07
#   -   retrieve rates from mysql if query is very recent (staff starting work)
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool

import sys, traceback
from celery.task import task, Task
from celery.task.sets import TaskSet
from celery.execute import send_task
import couchdb
from decimal import Decimal, ROUND_HALF_UP
import string

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import settings

from sqlalchemy.sql import text
try:
    from sqlalchemy import exceptions as ex_sqlalchemy
except:
    pass
from persistent_mysql_connection import engine
from pprint import pformat

from Cheetah.Template import Template


@task
def get_rates_from_couch_date_range(subcontractors_id, start_date_time, end_date_time):
    #datetime earlier than 2013-02-07 are potentially misleading before normans fix
    #just return blank list
    logging.info(start_date_time)
    logging.info(end_date_time)
    couch_records = []

    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']
    x = start_date_time
    y = end_date_time
    x = [x.year, x.month, x.day, x.hour, x.minute, x.second]
    y = [y.year, y.month, y.day, y.hour, y.minute, y.second]

    data = db.view('hourly_rate/subcon_id-datetime', startkey=[subcontractors_id, x], endkey=[subcontractors_id, y])

    for r in data:
        logging.info(r['value'])
        couch_records.append(dict(
            date_time = r['key'][1],
            hourly_rate = r['value']
        ))

    return couch_records


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


def compute_hourly_rate(client_price, work_status):
    """returns computed hourly rate
    >>> compute_hourly_rate(500, 'Part-Time')
    Decimal('5.77')
    >>> compute_hourly_rate(1200, None)
    Decimal('6.92')
    >>> compute_hourly_rate(393.9, 'Part-Time')
    Decimal('4.55')
    """
    if work_status == 'Part-Time':
        hours_per_day = Decimal('4.0')
    else:
        hours_per_day = Decimal('8.0')

    client_price = Decimal('%0.2f' % client_price)
    months_per_year = Decimal('12.0')
    weeks_per_year = Decimal('52.0')
    days_per_week = Decimal('5.0')

    staff_daily_rate = client_price * months_per_year / weeks_per_year / days_per_week
    hourly_rate = staff_daily_rate / hours_per_day

    return hourly_rate.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


@task
def get_rate_work_status_hours_per_day(subcontractors_id, datetime_req):
    """returns a dict of hourly_rate, work_status, client_price, hours_per_day
    """

    sql = text("""
        SELECT rate AS client_price, work_status 
        FROM subcontractors_client_rate
        WHERE subcontractors_id = :subcontractors_id
        AND start_date <= :datetime_req
        AND (end_date > :datetime_req OR end_date IS NULL)
        """)
    conn = engine.connect()
    data = conn.execute(sql, subcontractors_id=subcontractors_id, datetime_req=datetime_req).fetchall()

    if len(data) == 1:  #retrieve from subcontractors table
        client_price, work_status = data[0]
        if work_status in (None, ''): #old data, no associated work_status, get it from subcontractors table
            sql = text("""
                SELECT work_status
                FROM subcontractors
                WHERE id = :subcontractors_id
                """)
            data = conn.execute(sql, subcontractors_id=subcontractors_id).fetchone()
            work_status = data.work_status

    elif len(data) == 0:  #retrieve from subcontractors table
        sql = text("""
            SELECT client_price, work_status
            FROM subcontractors
            WHERE id = :subcontractors_id
            """)
        data = conn.execute(sql, subcontractors_id=subcontractors_id).fetchone()
        client_price, work_status = data
    else:
        send_task('notify_devs.send', ['multiple records found for subcontractors_client_rate','Please check subcontractors_id %s. Multiple records from subcontractors_client_rate found for date %s!' % (subcontractors_id, datetime_req)])
        client_price, work_status = data[0]

    if work_status == 'Part-Time':
        hours_per_day = Decimal('4.0')
    else:
        hours_per_day = Decimal('8.0')

    client_price = Decimal('%s' % client_price).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    hourly_rate = compute_hourly_rate(client_price, work_status)
    conn.close()

    return dict(
        hourly_rate = hourly_rate,
        work_status = work_status,
        client_price = client_price,
        hours_per_day = hours_per_day,
    )


def get_rates_from_mysql(subcontractors_id, datetime_req):
    """Fallback function querying mysql database
##~    >>> get_rates_from_mysql(3159, datetime(2012,11,7,0,0,0))
##~    Decimal('4.32')
    >>> get_rates_from_mysql(3000, datetime.now())
    Decimal('0.00')
    >>> get_rates_from_mysql(3159, datetime.now())
    Decimal('8.64')
    >>> get_rates_from_mysql(2714, datetime(2012,9,2,0,0,0))
    Decimal('4.11')
    >>> get_rates_from_mysql(3205, datetime(2012,11,21,0,0,0))
    Decimal('6.82')
    """

    sql = text("""
        SELECT rate AS client_price, work_status 
        FROM subcontractors_client_rate
        WHERE subcontractors_id = :subcontractors_id
        AND start_date <= :datetime_req
        AND (end_date > :datetime_req OR end_date IS NULL)
        """)
    conn = engine.connect()
    data = conn.execute(sql, subcontractors_id=subcontractors_id, datetime_req=datetime_req).fetchall()

    if len(data) == 1:  #retrieve from subcontractors table
        client_price, work_status = data[0]
        if work_status == None: #old data, no associated work_status, get it from subcontractors table
            sql = text("""
                SELECT work_status
                FROM subcontractors
                WHERE id = :subcontractors_id
                """)
            data = conn.execute(sql, subcontractors_id=subcontractors_id).fetchone()
            work_status = data.work_status

    elif len(data) == 0:  #retrieve from subcontractors table
        sql = text("""
            SELECT client_price, work_status
            FROM subcontractors
            WHERE id = :subcontractors_id
            """)
        data = conn.execute(sql, subcontractors_id=subcontractors_id).fetchone()
        client_price, work_status = data
    else:
        send_task('notify_devs.send', ['multiple records found for subcontractors_client_rate','Please check subcontractors_id %s. Multiple records from subcontractors_client_rate found for date %s!' % (subcontractors_id, datetime_req)])
        client_price, work_status = data[0]

    hourly_rate = compute_hourly_rate(client_price, work_status)
    conn.close()
    return hourly_rate


def get_rates_from_couch(subcontractors_id, datetime_req):
    """returns None if no data found from couchdbs rssc_time_records, otherwise return decimal
    since 2013-01-27 12:50:40, client_price were recorded on rssc_time_records
    >>> get_rates_from_couch(3000, datetime(2013,1,27,12,50,40))
    >>> get_rates_from_couch(2194, datetime(2013,1,28,6,58,57))
    >>> get_rates_from_couch(2194, datetime(2013,1,27,0,0,0))
    >>> get_rates_from_couch(2096, datetime(2013,1,28,6,30,35))
    """
    #datetime earlier than 2013-02-07 are potentially misleading before normans fix
    #just return None to force to query mysql
    if datetime_req <= datetime(2013,2,7,12,0,0):
        return None

    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']
    x = datetime_req
    y = x + timedelta(days=1)   #give one day allowance
    x = [x.year, x.month, x.day, x.hour, x.minute, x.second]
    y = [y.year, y.month, y.day, y.hour, y.minute, y.second]

    r = db.view('hourly_rate/subcon_id-datetime', startkey=[subcontractors_id, x], endkey=[subcontractors_id, y], limit=1)

    if len(r.rows) == 0:
        return None
    else:
        return Decimal('%s' % r.rows[0].value)


@task(ignore_result=True)
def check_leads_id_for_zero_hourly_rate(subcontractors_id):
    """
    given the subcontractors_id, retrieve the leads_id
    leads_id=11 / chris is excempted
    leads_id=7935 / rica is excempted
    """
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc']
    doc_id = 'subcon-%s' % subcontractors_id
    doc = db.get(doc_id)
    if doc == None:
        logging.info('check_leads_id_for_zero_hourly_rate cannot find document %s' % doc_id)
        send_task('notify_devs.send', ['celery error subcontractors.check_leads_id_for_zero_hourly_rate', 'Cannot find document %s' % doc_id])
        return
    leads_id = doc.get('leads_id')
    if leads_id not in [11, 7935]:  #chris and rica are excempted
        logging.info('check_leads_id_for_zero_hourly_rate hourly rate is 0 for %s' % doc_id)
        send_task('notify_devs.send', ['Hourly Rate is zero for sid:%s' % subcontractors_id, 'Please check rates!'])
        return


@task(ignore_result=True)
def compare_hourly_rates(subcontractors_id, current_hourly_rate):
    """
    no return value, just some notification if subcontractors record is not the same as current_hourly_rate
    """
    conn = engine.connect()
    sql = text("""
        SELECT client_price, work_status
        FROM subcontractors
        WHERE id = :subcontractors_id
        """)
    data = conn.execute(sql, subcontractors_id=subcontractors_id).fetchone()
    client_price, work_status = data
    hourly_rate = compute_hourly_rate(client_price, work_status)
    conn.close()

    if hourly_rate != current_hourly_rate:
        logging.info('compare_hourly_rates for sid:%s are not the same. Fetched: %s vs Recorded on subcontractors table:%s' % (subcontractors_id, current_hourly_rate, hourly_rate))
        send_task('notify_devs.send', ['Hourly Rates not the same for sid:%s' % subcontractors_id, 'Recorded on subcontractors table: %s\nCurrently used by staff: %s' % (hourly_rate, current_hourly_rate)])


@task
def get_hourly_rate(subcontractors_id, datetime_req):
    """given the subcontractors_id and datetime_req in Asia/Manila timezone
    return decimal hourly rate
    tables involved subcontractors_client_rate, subcontractors
    """
    try:
        #force to retrieve rates from mysql if datetime_req is very recent
        ph_time = get_ph_time()
        ph_time_past = ph_time - timedelta(minutes=30)
        ph_time += timedelta(minutes=5)
        if datetime_req >= ph_time_past and datetime_req <= ph_time:
            hourly_rate = get_rates_from_mysql(subcontractors_id, datetime_req)

            if hourly_rate == Decimal(0):
                check_leads_id_for_zero_hourly_rate(subcontractors_id)
            else:
                compare_hourly_rates(subcontractors_id, hourly_rate)

        else:
            hourly_rate = get_rates_from_couch(subcontractors_id, datetime_req)
            if hourly_rate == None:
                hourly_rate = get_rates_from_mysql(subcontractors_id, datetime_req)
    except Exception, exc:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.info(exc_type)
        logging.info(exc_value)
        logging.info('%r' % (traceback.extract_stack()))
        send_task('notify_devs.send', ['celery error subcontractors.get_hourly_rate', '%s\n%s\ntraceback:\n%s' % (exc_type, exc_value, repr(traceback.extract_stack()))])
        raise get_hourly_rate.retry(exc=exc)

    return hourly_rate


def run_tests():
    """
    >>> get_hourly_rate(3000, datetime.now())
    Decimal('0.00')
    >>> get_hourly_rate(3159, datetime.now())
    Decimal('8.64')
    >>> get_hourly_rate(2714, datetime(2012,9,2,0,0,0))
    Decimal('4.11')

    testing couchdb records before normans fix:
    >>> get_hourly_rate(2194, datetime(2013,1,28,6,58,57))
    Decimal('10.75')
    >>> get_hourly_rate(2096, datetime(2013,1,28,6,30,35))
    Decimal('7.10')

    testing couchdb records after normans fix:
    >>> get_rates_from_couch_date_range(2194, datetime(2013,2,8,0,0,0), datetime(2013,2,11,0,0,0))
    [{'date_time': [2013, 2, 8, 6, 58, 25], 'hourly_rate': '10.75'}]
    >>> get_rates_from_couch_date_range(3159, datetime(2013,2,2,0,0,0), datetime(2013,2,11,0,0,0))
    []
    >>> get_rates_from_couch_date_range(2714, datetime(2013,2,8,0,0,0), datetime(2013,2,11,0,0,0))
    [{'date_time': [2013, 2, 8, 6, 50, 10], 'hourly_rate': '5.08'}, {'date_time': [2013, 2, 9, 13, 51, 48], 'hourly_rate': '5.08'}]
    >>> get_rates_from_couch_date_range(3266, datetime(2013,2,8,0,0,0), datetime(2013,2,12,0,0,0))
    [{'date_time': [2013, 2, 8, 9, 16, 49], 'hourly_rate': '6.54'}, {'date_time': [2013, 2, 11, 9, 11, 37], 'hourly_rate': '5.83'}]

##~    >>> get_rate_work_status_hours_per_day(3159, datetime(2012,11,7,0,0,0))
##~    {'work_status': u'Full-Time', 'hours_per_day': Decimal('8.0'), 'client_price': Decimal('748.78'), 'hourly_rate': Decimal('4.32')}
    >>> get_rate_work_status_hours_per_day(3000, datetime.now())
    {'work_status': u'Full-Time', 'hours_per_day': Decimal('8.0'), 'client_price': Decimal('0.00'), 'hourly_rate': Decimal('0.00')}
    >>> get_rate_work_status_hours_per_day(3159, datetime.now())
    {'work_status': u'Full-Time', 'hours_per_day': Decimal('8.0'), 'client_price': Decimal('1497.56'), 'hourly_rate': Decimal('8.64')}
    >>> get_rate_work_status_hours_per_day(2714, datetime(2012,9,2,0,0,0))
    {'work_status': u'Full-Time', 'hours_per_day': Decimal('8.0'), 'client_price': Decimal('712.00'), 'hourly_rate': Decimal('4.11')}
    >>> get_rate_work_status_hours_per_day(3205, datetime(2012,11,21,0,0,0))
    {'work_status': u'Part-Time', 'hours_per_day': Decimal('4.0'), 'client_price': Decimal('591.46'), 'hourly_rate': Decimal('6.82')}
    >>> check_leads_id_for_zero_hourly_rate(3000)
    >>> check_leads_id_for_zero_hourly_rate(3205)
    >>> compare_hourly_rates(3000, Decimal('0'))
    >>> compare_hourly_rates(3159, Decimal('8.64'))
    >>> compare_hourly_rates(3159, Decimal('8.65'))

    """


if __name__ == "__main__":
    import doctest
    doctest.testmod()
