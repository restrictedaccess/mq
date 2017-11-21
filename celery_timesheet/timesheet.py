#   2014-03-06  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered leaves with status='absent'
#   2013-12-03  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix day on calendar on different timezone
#   2013-08-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add ROUND_HALF_UP on some decimal values
#   2013-08-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added retrieval of timesheet note
#   2013-08-19  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added prepaid status
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2013-01-07 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3437, update invoice description
#   2012-11-13 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   show a different description when qty is negative
#   -   fix qty
#   2012-11-03 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import couchdb
import re
import string
from pprint import pprint, pformat

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

import MySQLdb

from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from calendar import monthrange

from operator import itemgetter
from decimal import Decimal, ROUND_HALF_UP

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

WORKING_WEEKDAYS = 22
TWOPLACES = Decimal(10) ** -2



def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
    
def get_totals(timesheet_id):
    """
    given the timesheet_id, return total hours in decimal format
    """
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    conn = db.cursor()
    
    sql = """
        SELECT 
        SUM(regular_rostered)AS sum_regular_rostered,
        SUM(hrs_charged_to_client) AS sum_hrs_charged_to_client,
        SUM(adj_hrs) AS sum_adj_hrs,
        SUM(diff_charged_to_client) as sum_diff_charged_to_client
        FROM timesheet_details
        WHERE timesheet_id = %s
        """ % timesheet_id
    conn.execute(sql)
    sum_hours = dictfetchall(conn)
    sum_hours = sum_hours[0]
    
    conn.close()
    return_data = dict(
        sum_regular_rostered = sum_hours["sum_regular_rostered"],
        sum_hrs_charged_to_client = sum_hours["sum_hrs_charged_to_client"],
        sum_adj_hrs = sum_hours["sum_adj_hrs"],
        sum_diff_charged_to_client = sum_hours["sum_diff_charged_to_client"]
    )

    return return_data


@task
def get_invoice_items(client_id, month_year_str):
    """
    return a list of items suitable for invoicing
    """

    month_years = []
    x = datetime.strptime(month_year_str, '%Y-%m-%d')
    #make sure day 1
    y = date(x.year, x.month, 1)
    month_years.append(dict(
        adj = 'N',
        date_str = y.strftime('%Y-%m-%d')
    ))
    #subtract 1 day and get previous month/date
    z = y - timedelta(days=1)
    y = date(z.year, z.month, 1)
    month_years.append(dict(
        adj = 'Y',
        date_str = y.strftime('%Y-%m-%d')
    ))
    
    items = []

    db = MySQLdb.connect(**settings.DB_ARGS)
    conn = db.cursor()
    

    for month_year in month_years:
        
        
        month_year_str=month_year['date_str']
        #get timesheets
        sql = """
            SELECT t.id, t.month_year, t.userid, t.leads_id, t.subcontractors_id,
            p.fname, p.lname,
            s.job_designation, s.work_status, s.client_price
            FROM timesheet AS t
            LEFT JOIN personal AS p
            ON t.userid = p.userid
            LEFT JOIN subcontractors as s
            ON t.subcontractors_id = s.id
            WHERE t.leads_id = %s
            AND month_year = '%s'
            AND t.status != 'deleted'
            ORDER BY p.fname, p.lname
            """ % (client_id,  month_year_str)
        
        conn.execute(sql)
        timesheets = dictfetchall(conn)
    
        
        for ts in timesheets:
            totals = get_totals(ts["id"])

            if ts["work_status"] == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8

            total_hours = Decimal('%0.2f' % (WORKING_WEEKDAYS * hours_per_day))
            staff_hourly_rate = Decimal('%0.2f' % (ts["client_price"] * 12.0 / 52.0 / 5.0 / hours_per_day))

            description = '%s %s [%s]' % (ts["fname"], ts["lname"], ts["job_designation"])
            qty = totals['sum_hrs_charged_to_client']

            x = datetime.strptime(month_year['date_str'], '%Y-%m-%d')
            y, days = monthrange(x.year, x.month)
            start_date = [x.year, x.month, 1]
            end_date = [x.year, x.month, days]

            if month_year['adj'] == 'Y':
                x = datetime.strptime(month_year['date_str'], '%Y-%m-%d')
                y, days = monthrange(x.year, x.month)
                start_date = [x.year, x.month, 1]
                end_date = [x.year, x.month, days]
                sum_hrs_charged_to_client = totals['sum_hrs_charged_to_client']
                if sum_hrs_charged_to_client == None:
                    sum_hrs_charged_to_client = Decimal('0.0')
                sum_adj_hrs = totals['sum_adj_hrs']
                if sum_adj_hrs == None:
                    sum_adj_hrs = Decimal('0.0')

                qty = sum_adj_hrs - sum_hrs_charged_to_client

                #show a different description when qty is negative
                if qty < Decimal('0.0'):
                    description += ' (%s Un-Used Hour) Adjustment Credit Memo' % x.strftime('%B')
                else:
                    description += ' (%s Overtime) Adjustment Over Time Work' % x.strftime('%B')

            amount = staff_hourly_rate * qty

            item = dict(
                description = description,
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % qty,
                amount = '%0.2f' % amount,
                start_date = start_date,
                end_date = end_date
            )
            items.append(item)

    conn.close()
    return items


def get_time_records_between(conn, timerecords, start_date_time, end_date_time, subcontractors_id, timezone_ref, userid, leads_id, month_year, day, timesheet_details_id):
    """
    all time records are in Asia/Manila time
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """

    phtz = timezone('Asia/Manila')
    records = []
    total_work_hours = Decimal(0)
    total_lunch_hours = Decimal(0)

    for t in timerecords:
        sid = t['value'][3]
        record_type = t['value'][0]

        if record_type == 'quick break':
            continue

        if sid != subcontractors_id:
            continue
        a = t['key'][1]
        start = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=phtz)

        b = t['value'][1]
        if b == None or b == False:
            end = None
        else:
            end = datetime(b[0], b[1], b[2], b[3], b[4], b[5], tzinfo=phtz)

        if start_date_time < start and end_date_time > start:
            start = start.astimezone(timezone_ref)
            if end != None:
                end = end.astimezone(timezone_ref)

                #update totals
                time_diff = end - start
                time_diff_decimal = Decimal('%s' % (time_diff.seconds / 3600.0)).quantize(TWOPLACES, rounding=ROUND_HALF_UP)

                if record_type == 'time record':
                    total_work_hours += time_diff_decimal
                elif record_type == 'lunch record':
                    total_lunch_hours += time_diff_decimal
                    total_work_hours -= time_diff_decimal
                end_str = end.strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_str = None

            records.append(dict(
                start = start.strftime('%Y-%m-%d %H:%M:%S'),
                end = end_str,
                record_type = record_type
                ))

    notes = []
    #notes related from leave request
    #its more efficient to filter from leave_request_dates table first
    
    leave_request_start_date = month_year.replace(day=day, hour=0, minute=0, second=0)
    leave_request_end_date = month_year.replace(day=day, hour=23, minute=59, second=59)
    
    sql = """
        SELECT ld.date_of_leave, l.leave_type, l.date_requested, ld.status
        FROM leave_request_dates AS ld
        JOIN leave_request AS l
        ON ld.leave_request_id = l.id
        WHERE ld.status IN ('approved', 'absent')
        AND l.userid = %s
        AND l.leads_id = %s
        AND date_of_leave BETWEEN '%s' AND '%s'
    """ % (userid, leads_id, leave_request_start_date, leave_request_end_date)

    

    conn.execute(sql)
    data = dictfetchall(conn)
    
    for d in data:
        x = d["date_requested"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(timezone_ref)
        if d["status"] == 'approved':
            note = 'ON LEAVE : %s' % d["leave_type"]
        else:
            note = 'ABSENT'
        
        notes.append(dict(
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = 'LEAVE REQUEST SYSTEM',
                lname = '',
                note = note
            )
        )

    
    #notes from timesheet_notes_subcon
    sql = """
        SELECT t.note, t.timestamp, p.fname, p.lname, t.working_hrs, t.notes_category, t.file_name, t.id
        FROM timesheet_notes_subcon AS t
        JOIN personal AS p
        ON t.userid = p.userid
        WHERE timesheet_details_id = %s
    """ % timesheet_details_id    
    conn.execute(sql)
    data = dictfetchall(conn)
    
    for d in data:
        x = d["timestamp"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(timezone_ref)
        notes.append(dict(
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = d["fname"],
                lname = d["lname"],
                note = d["note"],
                notes_category = d["notes_category"],
                file_name = d["file_name"],
                working_hrs = d["working_hrs"],
                id = d["id"]
            )
        )
    
    #notes from timesheet_notes_admin
    sql = """
        SELECT t.note, t.timestamp, a.admin_fname AS fname, a.admin_lname AS lname
        FROM timesheet_notes_admin AS t
        JOIN admin AS a
        ON t.admin_id = a.admin_id
        WHERE timesheet_details_id = %s
    """ % timesheet_details_id
    conn.execute(sql)
    data = dictfetchall(conn)
    
    for d in data:
        x = d["timestamp"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(timezone_ref)
        notes.append(dict(
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = d["fname"],
                lname = d["lname"],
                note = d["note"],
                is_admin = True
            )
        )

    notes = sorted(notes, key=itemgetter('timestamp'))

    return dict(
        total_work_hours = total_work_hours,
        total_lunch_hours = total_lunch_hours,
        records = records,
        notes = notes,
    )


@task
def get_timesheet_details(timesheet_id, timezone_ref=None):
    
    logging.info('executing timesheet.get_timesheet_details %s' % timesheet_id)
    
    """
    timezone_ref is string type
    depending on timezone_ref, crossover hours might be affected
    must return all times/date based on timezone_ref
    """
    s = couchdb.Server(settings.COUCH_DSN)
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    conn = db.cursor()
    
    sql = """
        SELECT t.month_year, t.userid, t.leads_id, t.status AS timesheet_status, tz.timezone, t.subcontractors_id, s.prepaid
        FROM timesheet AS t
        LEFT JOIN timezone_lookup AS tz        
        ON t.timezone_id = tz.id
        LEFT JOIN subcontractors AS s
        ON s.id = t.subcontractors_id
        WHERE t.id = %s
    """ % timesheet_id
    conn.execute(sql)
    timesheet_result = dictfetchall(conn)
    timesheet =  timesheet_result[0]
    
    month_year = timesheet['month_year']
    userid = timesheet['userid']
    ts_timezone = timesheet['timezone']
    subcontractors_id = timesheet['subcontractors_id']
    leads_id = timesheet['leads_id']
    prepaid = timesheet['prepaid']
    
    if timezone_ref == None:
        timezone_ref = ts_timezone

    if ts_timezone == None or ts_timezone == False:
        ts_timezone = timezone('Asia/Manila')
    else:
        ts_timezone = timezone(ts_timezone)

    phtz = timezone('Asia/Manila')
    
    
    #get details
    sql = """
        SELECT id, timesheet_id, day, total_hrs, adj_hrs, regular_rostered, hrs_charged_to_client, diff_charged_to_client, hrs_to_be_subcon, diff_paid_vs_adj_hrs, notes_locked_date
        FROM timesheet_details
        WHERE timesheet_id = %s
    """ % timesheet_id
    conn.execute(sql)
    timesheet_details = dictfetchall(conn)
    
    timezone_ref = timezone(timezone_ref)

    start_date = datetime(month_year.year, month_year.month, month_year.day, 0, 0, 0, tzinfo=timezone_ref)
    end_date = datetime(month_year.year, month_year.month, timesheet_details[-1]['day'], 23, 59, 59, tzinfo=timezone_ref)

    #convert to Asia/Manila tz
    start_date = start_date.astimezone(phtz)
    end_date = end_date.astimezone(phtz)

    a = start_date.timetuple()
    a = [a[0], a[1], a[2], a[3], a[4], a[5]]
    b = end_date.timetuple()
    b = [b[0], b[1], b[2], b[3], b[4], b[5]]

    db = s['rssc_time_records']
    timerecords = db.view('rssc_reports/userid_timein', startkey=[userid, a], endkey=[userid, b]).rows

    #grand_totals
    grand_total_hrs = Decimal(0)
    grand_total_adj_hrs = Decimal(0)
    grand_total_reg_ros_hrs = Decimal(0)
    grand_total_hrs_charged_to_client = Decimal(0)
    grand_total_diff_charged_to_client = Decimal(0)
    grand_total_hrs_to_be_subcon = Decimal(0)
    grand_total_lunch_hrs = Decimal(0)
    grand_total_diff_pay_adj = Decimal(0)
    
    ts_details = []
    str =""
    for t in timesheet_details:
        
        #del t["status"], t["reference_date"]
        #convert the days into Philippine timezone
        a_cal = datetime(month_year.year, month_year.month, t["day"], 0, 0, 0, tzinfo=timezone_ref)
        b = datetime(month_year.year, month_year.month, t["day"], 23, 59, 59, tzinfo=timezone_ref)
        a = a_cal.astimezone(phtz)
        b = b.astimezone(phtz)
        
        data = get_time_records_between(conn, timerecords, a, b, subcontractors_id, timezone_ref, userid, leads_id, month_year, t["day"], t["id"])
        #str += data
        c = dict(t.items())

        if t['notes_locked_date'] != None:
            c['notes_locked_date'] = t['notes_locked_date'].strftime('%Y-%m-%d %H:%M:%S')

        c.pop('adj_hrs')
        c.pop('regular_rostered')
        c['records'] = data['records']
        c['notes'] = data['notes']
        c['date'] = '%0.2d' % t["day"]
        c['day'] = a_cal.strftime('%a')

        time_in = []
        time_out = []
        lunch_in = []
        lunch_out = []

        for record in data['records']:
            if record['record_type'] == 'time record':
                start = datetime.strptime(record['start'], '%Y-%m-%d %H:%M:%S').strftime('%m/%d %H:%M')
                time_in.append(start)
                if record['end'] != None:
                    end = datetime.strptime(record['end'], '%Y-%m-%d %H:%M:%S').strftime('%m/%d %H:%M')
                    time_out.append(end)

            elif record['record_type'] == 'lunch record':
                start = datetime.strptime(record['start'], '%Y-%m-%d %H:%M:%S').strftime('%m/%d %H:%M')
                lunch_in.append(start)
                if record['end'] != None:
                    end = datetime.strptime(record['end'], '%Y-%m-%d %H:%M:%S').strftime('%m/%d %H:%M')
                    lunch_out.append(end)

        
        c['time_in'] = time_in
        c['time_out'] = time_out
        c['lunch_in'] = lunch_in
        c['lunch_out'] = lunch_out

        #need to convert decimals into string
        total_hrs = data['total_work_hours']
        c['total_hrs'] = '%0.2f' % total_hrs
        grand_total_hrs += total_hrs.quantize(TWOPLACES, rounding=ROUND_HALF_UP)

        lunch_hours = data['total_lunch_hours']
        c['lunch_hours'] = '%0.2f' % lunch_hours
        grand_total_lunch_hrs += lunch_hours

        adj_hrs = t["adj_hrs"]
        if adj_hrs == None:
            adj_hrs = Decimal(0)
        c['adjusted_hrs'] = '%0.2f' % adj_hrs
        grand_total_adj_hrs += adj_hrs

        regular_rostered = t["regular_rostered"]
        if regular_rostered == None:
            regular_rostered = Decimal(0)
        c['regular_rostered_hrs'] = '%0.2f' % regular_rostered
        grand_total_reg_ros_hrs += regular_rostered

        hrs_charged_to_client = t["hrs_charged_to_client"]
        if hrs_charged_to_client == None:
            hrs_charged_to_client = Decimal(0)
        c['hrs_charged_to_client'] = '%0.2f' % hrs_charged_to_client
        grand_total_hrs_charged_to_client += hrs_charged_to_client

        diff_charged_to_client = t["diff_charged_to_client"]
        if diff_charged_to_client == None:
            diff_charged_to_client = Decimal(0)
        c['diff_charged_to_client'] = '%0.2f' % diff_charged_to_client
        grand_total_diff_charged_to_client += diff_charged_to_client

        hrs_to_be_subcon = t["hrs_to_be_subcon"]
        if hrs_to_be_subcon == None:
            hrs_to_be_subcon = Decimal(0)
        c['hrs_to_be_subcon'] = '%0.2f' % hrs_to_be_subcon
        grand_total_hrs_to_be_subcon += hrs_to_be_subcon

        diff_pay_adj = adj_hrs - hrs_to_be_subcon
        c['diff_pay_adj'] = '%0.2f' % diff_pay_adj
        grand_total_diff_pay_adj += diff_pay_adj

        ts_details.append(c)


    timesheet = dict(timesheet.items())
    timesheet['timesheet_details'] = ts_details
    timesheet['month_year'] = timesheet['month_year'].strftime('%Y-%m-%d %H:%M:%S')

    timesheet_totals = {}
    timesheet_totals['grand_total_hrs'] = '%0.2f' % grand_total_hrs.quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    timesheet_totals['grand_total_adj_hrs'] = '%0.2f' % grand_total_adj_hrs
    timesheet_totals['grand_total_reg_ros_hrs'] = '%0.2f' % grand_total_reg_ros_hrs
    timesheet_totals['grand_total_hrs_charged_to_client'] = '%0.2f' % grand_total_hrs_charged_to_client 
    timesheet_totals['grand_total_diff_charged_to_client'] = '%0.2f' % grand_total_diff_charged_to_client
    timesheet_totals['grand_total_hrs_to_be_subcon'] = '%0.2f' % grand_total_hrs_to_be_subcon
    timesheet_totals['grand_total_lunch_hrs'] = '%0.2f' % grand_total_lunch_hrs
    timesheet_totals['grand_total_diff_pay_adj'] = '%0.2f' % grand_total_diff_pay_adj

    timesheet['timesheet_totals'] = timesheet_totals
    timesheet['prepaid'] = prepaid
    
    
    conn.close()
    return timesheet
    


@task
def get_timesheet_notes(timesheet_details_id, timezone_ref):
    #get ts
    db = MySQLdb.connect(**settings.DB_ARGS)
    conn = db.cursor()
    
    sql = """
        SELECT day, timesheet_id 
        FROM timesheet_details
        WHERE id = %s
    """ % timesheet_details_id
    
    conn.execute(sql)
    data = dictfetchall(conn)
    data =  data[0]
    
    phtz = timezone('Asia/Manila')
    ref_tz = timezone(timezone_ref)
    
    ts_id = data["timesheet_id"]
    day = data["day"]

    sql = """
        SELECT month_year, userid, leads_id
        FROM timesheet
        WHERE id = %s
    """ % ts_id
    conn.execute(sql)
    data = dictfetchall(conn)
    data =  data[0]
    
    month_year = data["month_year"]
    userid = data["userid"]
    leads_id = data["leads_id"]

    leave_request_start_date = month_year.replace(day=day, hour=0, minute=0, second=0)
    leave_request_end_date = month_year.replace(day=day, hour=23, minute=59, second=59)

    notes = []
    sql = """
        SELECT ld.date_of_leave, l.leave_type, l.date_requested, ld.status
        FROM leave_request_dates AS ld
        JOIN leave_request AS l
        ON ld.leave_request_id = l.id
        WHERE ld.status IN ('approved', 'absent')
        AND l.userid = %s
        AND l.leads_id = %s
        AND date_of_leave BETWEEN '%s' AND '%s'
    """ % (userid, leads_id, leave_request_start_date, leave_request_end_date)
    conn.execute(sql)
    data = dictfetchall(conn)
    
    #data = conn.execute(sql, userid=userid, leads_id=leads_id, leave_request_start_date=leave_request_start_date, leave_request_end_date=leave_request_end_date)
    for d in data:
        x = d["date_requested"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(ref_tz)
        if d["status"] == 'approved':
            note = 'ON LEAVE : %s' % d["leave_type"]
        else:
            note = 'ABSENT'
        
        notes.append(dict(
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = 'LEAVE REQUEST SYSTEM',
                lname = '',
                note = note
            )
        )

    #notes from timesheet_notes_subcon
    sql = """
        SELECT t.note, t.timestamp, p.fname, p.lname, t.working_hrs, t.notes_category, t.file_name
        FROM timesheet_notes_subcon AS t
        JOIN personal AS p
        ON t.userid = p.userid
        WHERE timesheet_details_id = %s
    """ % timesheet_details_id
    conn.execute(sql)
    data = dictfetchall(conn)
    
    for d in data:
        x = d["timestamp"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(ref_tz)
        notes.append(dict(
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = d["fname"],
                lname = d["lname"],
                note = d["note"],
                notes_category = d["notes_category"],
                file_name = d["file_name"],
                working_hrs = d["working_hrs"]
            )
        )

    #notes from timesheet_notes_admin
    sql = """
        SELECT t.note, t.timestamp, a.admin_fname AS fname, a.admin_lname AS lname
        FROM timesheet_notes_admin AS t
        JOIN admin AS a
        ON t.admin_id = a.admin_id
        WHERE timesheet_details_id = %s
    """ % timesheet_details_id
    
    conn.execute(sql)
    data = dictfetchall(conn)
    for d in data:
        x = d["timestamp"]
        y = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=phtz)
        timestamp = y.astimezone(ref_tz)
        notes.append(dict(
                id = d["id"],
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                fname = d["fname"],
                lname = d["lname"],
                note = d["note"],
                is_admin = True
            )
        )

    notes = sorted(notes, key=itemgetter('timestamp'))
    conn.close()

    return notes


def run_tests():
    """
    >>> #get_totals(13800) == {'sum_diff_charged_to_client': Decimal('9.21'), 'sum_hrs_charged_to_client': Decimal('184.00'), 'sum_adj_hrs': Decimal('193.21'), 'sum_regular_rostered': Decimal('184.00')}
    True
    >>> #get_totals(13821) == {'sum_diff_charged_to_client': Decimal('-3.66'), 'sum_hrs_charged_to_client': Decimal('184.00'), 'sum_adj_hrs': Decimal('180.34'), 'sum_regular_rostered': Decimal('184.00')}
    True
    >>> #get_invoice_items(3847, '2012-11-01') == [{'amount': '1723.04', 'description': u'Darren Denver Alba [FT Web Developer]', 'end_date': [2012, 11, 30], 'start_date': [2012, 11, 1], 'unit_price': '9.79', 'qty': '176.00'}, {'amount': '1371.04', 'description': u'Mark Arjay Acoba [Full Time Web Developer]', 'end_date': [2012, 11, 30], 'start_date': [2012, 11, 1], 'unit_price': '7.79', 'qty': '176.00'}, {'amount': '1726.56', 'description': u'Mark Thomas Tiangco [PART TIME .NET DEVELOPER]', 'end_date': [2012, 11, 30], 'start_date': [2012, 11, 1], 'unit_price': '9.81', 'qty': '176.00'}, {'amount': '1928.96', 'description': u'Melissa Gonzales [PT Web Developer]', 'end_date': [2012, 11, 30], 'start_date': [2012, 11, 1], 'unit_price': '10.96', 'qty': '176.00'}, {'amount': '561.06', 'description': u'Darren Denver Alba [FT Web Developer] (October Overtime) Adjustment Over Time Work', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '9.79', 'qty': '57.31'}, {'amount': '39.34', 'description': u'Mark Arjay Acoba [Full Time Web Developer] (October Overtime) Adjustment Over Time Work', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '7.79', 'qty': '5.05'}, {'amount': '32.86', 'description': u'Mark Thomas Tiangco [PART TIME .NET DEVELOPER] (October Overtime) Adjustment Over Time Work', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '9.81', 'qty': '3.35'}, {'amount': '-88.67', 'description': u'Melissa Gonzales [PT Web Developer] (October Un-Used Hour) Adjustment Credit Memo', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '10.96', 'qty': '-8.09'}]
    True
    >>> #get_invoice_items(4208, '2012-10-01') == [{'amount': '2426.96', 'description': u'Dean Earl Bartolabac [FT Senior Web Developer]', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '13.19', 'qty': '184.00'}, {'amount': '2044.24', 'description': u'Edward Allan  Florindo [FT Php Developer]', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '11.11', 'qty': '184.00'}, {'amount': '1698.32', 'description': u'Juan Ringor [FULL TIME PhP DEVELOPER]', 'end_date': [2012, 10, 31], 'start_date': [2012, 10, 1], 'unit_price': '9.23', 'qty': '184.00'}, {'amount': '-21.24', 'description': u'Dean Earl Bartolabac [FT Senior Web Developer] (September Un-Used Hour) Adjustment Credit Memo', 'end_date': [2012, 9, 30], 'start_date': [2012, 9, 1], 'unit_price': '13.19', 'qty': '-1.61'}, {'amount': '-102.10', 'description': u'Edward Allan  Florindo [FT Php Developer] (September Un-Used Hour) Adjustment Credit Memo', 'end_date': [2012, 9, 30], 'start_date': [2012, 9, 1], 'unit_price': '11.11', 'qty': '-9.19'}, {'amount': '0.00', 'description': u'Juan Ringor [FULL TIME PhP DEVELOPER] (September Overtime) Adjustment Over Time Work', 'end_date': [2012, 9, 30], 'start_date': [2012, 9, 1], 'unit_price': '9.23', 'qty': '0.00'}]
    True
    >>> #items = get_invoice_items(11, '2012-10-01')
    >>> #logging.info(pformat(items, 4))
    >>> #get_timesheet_details(17076, 'Asia/Manila')

    >>> #get_timesheet_details(16922, 'Australia/Sydney')
    >>> #get_timesheet_details(17070, 'America/New_York')
    >>> #get_timesheet_details(17173, 'Asia/Manila') #has cross over dates

    #testing for timesheet note
    >>> get_timesheet_notes(584802,'Asia/Manila')
    []

    >>> get_timesheet_notes(584802,'Australia/Sydney')
    []

    >>> get_timesheet_notes(584802,'Europe/London')
    []

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
