#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated particulars depending on term
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-02-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed 'overtime or' as per ricas request
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2013-01-15 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close connection and dispose engine
#   2012-11-23 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bug fix on timezone assignment/comparison
#   2012-10-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   dont create adjustment when timesheet status is deleted
#   2012-09-28 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed the word undertime as per ricas request
#   2012-08-29 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fixed computation of hours

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

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import settings 

couch_server = couchdb.Server(settings.COUCH_DSN)
db_rssc = couch_server['rssc']
db_rssc_time_records = couch_server['rssc_time_records']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')

DOC_VERSION = '2012-11-23'

def get_ph_time():
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


@task
def GetHours(timesheet, new_cutoff_date_string, currency_couch, apply_gst_couch):
    """returns total_hours_worked and total_adj_hours
    """
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_rssc = couch_server['rssc']
    conn = engine.connect()

    now = get_ph_time()
    if timesheet.timezone_id == None:
        timesheet_tz = timezone('Asia/Manila')
    else:
        s = text("""SELECT timezone from timezone_lookup
            where id = :tz_id
            """)
        r = conn.execute(s, tz_id = timesheet.timezone_id).fetchone()
        timesheet_tz = timezone(r.timezone)

    doc_subcontractors = db_rssc.get('subcon-%s' % timesheet.subcontractors_id)
    if doc_subcontractors == None:
        return

    my = timesheet.month_year

    if doc_subcontractors.has_key('prepaid_cutoffs') == False:
        last_cutoff_date = timesheet_tz.localize(datetime(my.year,my.month,1,0,0,0,0))
    else:
        k = doc_subcontractors['prepaid_cutoffs'].keys()
        k.sort(reverse=True)
        a = datetime.strptime(k[0], '%Y-%m-%d')
        last_cutoff_date = timesheet_tz.localize(datetime(a.year, a.month, a.day, 0, 0, 0, 0))
        #add one day, last_cutoff_date is inclusive
        last_cutoff_date += timedelta(days=1)

    date_start = timesheet_tz.localize(datetime(my.year, my.month, 1, 0, 0, 0, 0))
    x = calendar.monthrange(my.year, my.month)  #get number of days
    date_end = timesheet_tz.localize(datetime(my.year, my.month, x[1], 0, 0, 0, 0))

    if last_cutoff_date > date_start:   #do not include timesheet anymore
        if last_cutoff_date.month != date_start.month:
            return
        else:   #except same month
            date_start = last_cutoff_date

    new_cutoff_date = datetime.strptime(new_cutoff_date_string, '%Y-%m-%d')
    new_cutoff_date = timesheet_tz.localize(new_cutoff_date)

    if new_cutoff_date < date_end:
        date_end = new_cutoff_date

    sql = text("""SELECT * FROM timesheet_details
        WHERE timesheet_id = :timesheet_id
        AND day >= :day_start
        AND day <= :day_end
        ORDER BY day
        """)
    timesheet_details = conn.execute(sql, timesheet_id=timesheet.id, day_start=date_start.day, day_end=date_end.day).fetchall()

    staff_hourly_rate = Decimal(0)
    total_adj_hours = Decimal(0)
    total_hours_worked = Decimal(0)

    #get the hourly rate    
    work_status = timesheet.work_status
    if work_status == 'Part-Time':
        hours_per_day = 4
    else:
        hours_per_day = 8

    #TODO this would be updated once the price is recorded
    staff_hourly_rate = Decimal('%0.2f' % (timesheet.client_price * 12 / 52 / 5 / hours_per_day))

    #optimize this loop by passing to celery
    tasks = []
    for timesheet_detail in timesheet_details:
        x = GetHoursWorked.subtask((timesheet_detail, my, timesheet_tz, timesheet, last_cutoff_date, new_cutoff_date))
        tasks.append(x)

    #end timesheet_detail loop

    total_adj_hours = Decimal(0)
    total_hours_worked = Decimal(0)

    job = TaskSet(tasks = tasks)
    result = job.apply_async()
    data = result.join()

    for a in data:
        total_adj_hours += a['total_adj_hours']
        total_hours_worked += a['total_hours_worked']

    diff_hours = total_adj_hours - total_hours_worked 

    #apply credit or charge
    if diff_hours == Decimal(0):
        term = 'credit'
    elif diff_hours > 0:
        term = 'charge'
    else:
        term = 'credit'
        diff_hours = diff_hours * Decimal(-1)

    diff_hours_amt = diff_hours * staff_hourly_rate     #TODO is inappropriate if the schedules of hourly rate is followed

    if apply_gst_couch == 'Y':
        gst = (diff_hours_amt * Decimal('0.1'))
        diff_hours_amt += gst

    timesheet_data = dict(
        client_price = '%0.2f' % timesheet.client_price,
        last_cutoff_date = last_cutoff_date.strftime('%F'),
        new_cutoff_date = new_cutoff_date.strftime('%F'),
        timesheet_id = timesheet['id'],
        timesheet_timezone = timesheet_tz.zone,
        month_year = timesheet.month_year.strftime('%F'),
        total_adj_hours = '%0.2f' % total_adj_hours,
        total_hours_worked = '%0.2f' % total_hours_worked, 
        leads_id = timesheet['leads_id'],
        staff_hourly_rate = '%0.2f' % staff_hourly_rate,
        currency_couch = currency_couch,
        apply_gst_couch = apply_gst_couch,
        currency_subcontractors = timesheet.currency,
        apply_gst_leads = timesheet.apply_gst,
        date_start = date_start.strftime('%F'),
        date_end = date_end.strftime('%F'),
        diff_hours = '%0.2f' % diff_hours,
        diff_hours_amt = '%0.2f' % diff_hours_amt,
        term = term,
        date_time_referenced = '%s' % now,
        subcontractors_id = timesheet['subcontractors_id'],
    )

    gst = ''
    if apply_gst_couch == 'Y':
        gst = '+GST'

    #display for mass generation
    timesheet_data['desc'] = '%s %s [%s] (%s to %s) Adjustments @ %s/hr%s hrs total:%s adj:%s %s:%s' % (
        timesheet.fname,
        timesheet.lname,
        timesheet.job_designation,
        date_start.strftime('%b %d, %Y'),
        date_end.strftime('%b %d, %Y'),
        '%0.2f' % staff_hourly_rate,
        gst,
        '%0.2f' % total_hours_worked,
        '%0.2f' % total_adj_hours,
        term,
        '%0.2f' % diff_hours,
        )

    #particular for adjustment
    if term == 'charge':
        particular = 'Staff %s %s, <%s> total worked hours adjustments due to offline work between %s and %s. Adjusted Hours is %s %s' % (
            timesheet.fname,
            timesheet.lname,
            timesheet.job_designation,
            date_start.strftime('%b %d, %Y'),
            date_end.strftime('%b %d, %Y'),
            '%0.2f' % diff_hours,
            gst,
            )
    else:
        particular = 'Staff %s %s, <%s> total worked hours adjustments due to unapproved worked hours between %s and %s. Adjusted Hours is %s %s' % (
            timesheet.fname,
            timesheet.lname,
            timesheet.job_designation,
            date_start.strftime('%b %d, %Y'),
            date_end.strftime('%b %d, %Y'),
            '%0.2f' % diff_hours,
            gst,
            )

    timesheet_data['particular'] = particular

    #remarks for adjustment
    timesheet_data['remarks'] = 'Please refer to %s %s\'s Time Sheet for detailed breakdown.' % (
        timesheet.fname,
        timesheet.lname
    )

    conn.close()
    return timesheet_data


@task
def GetHoursWorked(timesheet_detail, my, timesheet_tz, timesheet, last_cutoff_date, new_cutoff_date):
    couch_server = couchdb.Server(settings.COUCH_DSN)
    _db_rssc_time_records = couch_server['rssc_time_records']

    staff_hourly_rate = Decimal(0)
    total_adj_hours = Decimal(0)
    total_hours_worked = Decimal(0)

    timesheet_detail_date = timesheet_tz.localize(datetime(my.year, my.month, timesheet_detail.day, 0, 0, 0, 0))

    if last_cutoff_date >= timesheet_detail_date:
        date_start = timesheet_detail_date

    if new_cutoff_date >= timesheet_detail_date:
        date_end = timesheet_detail_date

    if timesheet_detail_date >= last_cutoff_date and \
        timesheet_detail_date <= new_cutoff_date:

        if timesheet_detail.adj_hrs != None:
            total_adj_hours += timesheet_detail.adj_hrs

        #get total_hours_worked
        a = timesheet_detail_date.astimezone(phtz)
        start_ph_time = datetime(a.year, a.month, a.day, a.hour, a.minute, a.second)
        end_ph_time = start_ph_time + timedelta(days = 1) - timedelta(seconds = 1)

        #get all timerecords via couchdb
        hours_per_day_couch = timedelta(seconds=0)
        startkey = [int(timesheet['userid']), int(timesheet['subcontractors_id']), [start_ph_time.year, start_ph_time.month, start_ph_time.day, start_ph_time.hour, start_ph_time.minute, start_ph_time.second]]
        endkey = [int(timesheet['userid']), int(timesheet['subcontractors_id']), [end_ph_time.year, end_ph_time.month, end_ph_time.day, end_ph_time.hour, end_ph_time.minute, end_ph_time.second]]

        view = _db_rssc_time_records.view('prepaid/timerecords', startkey=startkey, endkey=endkey)

        if view == None:
            return dict(total_adj_hours=total_adj_hours, 
                total_hours_worked=total_hours_worked)

        for r in view:
            if r == None:
                continue

            if r.key == None:
                continue

            userid, subcontractors_id, a = r.key
            b, leads_id = r.value

            if b == None or b == False:
                continue    #timerecord not closed yet

            x = datetime(a[0], a[1], a[2], a[3], a[4], a[5]) 
            y = datetime(b[0], b[1], b[2], b[3], b[4], b[5])

            hours_per_day_couch += y - x

            #get lunch records
            startkey_lunch = [int(timesheet['userid']), int(timesheet['subcontractors_id']), a]
            endkey_lunch = [int(timesheet['userid']), int(timesheet['subcontractors_id']), b]
            view_lunch = _db_rssc_time_records.view('prepaid/lunchrecords', startkey=startkey_lunch, endkey=endkey_lunch)

            if view_lunch == None:
                continue

            for r_lunch in view_lunch:
                if r_lunch == None:
                    continue
                if r_lunch.key == None:
                    continue

                userid, subcontractors_id, a = r_lunch.key
                b, leads_id = r_lunch.value

                x = datetime(a[0], a[1], a[2], a[3], a[4], a[5]) 
                y = datetime(b[0], b[1], b[2], b[3], b[4], b[5])

                hours_per_day_couch -= y - x

        total_hours_worked += Decimal('%0.2f' % (hours_per_day_couch.seconds / 3600.0))

    return dict(total_adj_hours=total_adj_hours, 
        total_hours_worked=total_hours_worked)


@task
def CreatePrepaidAdjustment(leads_id, new_cutoff_date_string):
    """given the leads_id, collect all timesheets for adjustment
    """
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_client_settings = couch_server['client_docs']

    now = get_ph_time()
    first_day = date(now.year, now.month, 1)
    first_day = first_day - timedelta(days=settings.TIMESHEET_DAYS_TO_CONSIDER)

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
        AND t.leads_id = :leads_id
        AND t.status in ('open', 'locked')
        ORDER BY p.fname, t.month_year
        """)

    conn = engine.connect()
    timesheets = conn.execute(sql, first_day=first_day.strftime('%F %H:%M:%S'), new_cutoff_date=new_cutoff_date.strftime('%F %H:%M:%S'), leads_id=leads_id).fetchall()

    data = []
    if len(timesheets) > 0:
        #get client settings
        now_array = [now.year, now.month, now.day, now.minute, now.second, 0]
        r = db_client_settings.view('client/settings', startkey=[leads_id, now_array],
            endkey=[leads_id, [2011,1,1,0,0,0,0]],
            descending=True, limit=1)

        if len(r.rows) == 0:
            currency_couch = 'Not Found'
            apply_gst_couch = 'N'
        else:
            currency_couch, apply_gst_couch = r.rows[0]['value']

        for timesheet in timesheets:
            if timesheet == None:
                continue
            result = GetHours.delay(timesheet, new_cutoff_date_string, currency_couch, apply_gst_couch)
            timesheet_data = result.get()
            if timesheet_data == None:
                continue

            data.append(timesheet_data)

        conn.close()
    return data


@task(ignore_result=True)
def CreatePrepaidAdjustments(leads_ids, new_cutoff_date_string, requested_by):
    """given the leads_ids, create TaskSet
    """
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_client_settings = couch_server['client_docs']

    s = couchdb.Server(settings.COUCH_DSN)
    db = s['prepaid_adjustments']
    doc_build = db.get('build')
    if doc_build != None:
        if doc_build['busy'] == 'Y':
            message = 'System is busy building adjustments since %s\nfor client_ids %s\nby %s.\nPlease try again at a later time.' % (doc_build['requested_date'], doc_build['leads_ids'], doc_build['requested_by'])
            return
    else:
        doc_build = {'_id':'build'}

    doc_build['busy'] = 'Y'
    doc_build['requested_by'] = requested_by
    doc_build['requested_date'] = '%s' % get_ph_time()
    doc_build['leads_ids'] = leads_ids
    if doc_build.has_key('finished_request_date'):
        doc_build.pop('finished_request_date')
    db.save(doc_build)

    for leads_id in leads_ids:
        #remove open docs relative to leads_id
        r = db.view('adjustments/open', key=int(leads_id), include_docs=True)
        for d in r:
            doc_open = d.doc
            doc_open['status'] = 'closed'
            doc_open['closed_date'] = '%s' % get_ph_time()
            doc_open['closed_by'] = requested_by
            db.save(doc_open)

        #save results
        result = CreatePrepaidAdjustment.delay(leads_id, new_cutoff_date_string)
        try:
            data = result.get()
            for i in range(len(data)):
                #prevent generating if new_cutoff_date is older than last_cutoff_date
                x = datetime.strptime(data[i]['new_cutoff_date'], '%Y-%m-%d')
                y = datetime.strptime(data[i]['last_cutoff_date'], '%Y-%m-%d')
                if x <= y:
                    continue
    
                doc = dict(
                    date_time_generated = '%s' % get_ph_time(),
                    generated_by = requested_by,
                    leads_id = leads_id,
                    data = data[i],
                    sort = i,
                    status = 'open',
                    cutoff_date = new_cutoff_date_string,
                    version = DOC_VERSION,
                )
                doc['type'] = 'adjustment'
                db.save(doc)
        except:
            logging.info('Failed finding client_docs for leads %s' % leads_id)
            pass

    #set doc_build to not busy
    doc_build['busy'] = 'N'
    doc_build['finished_request_date'] = '%s' % get_ph_time()
    db.save(doc_build)


