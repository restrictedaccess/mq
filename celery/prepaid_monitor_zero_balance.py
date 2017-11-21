#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
"""This script is run via celery scheduled task
for monitoring and forcing the staff to finish work and disconnect
"""
from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet
import couchdb
from decimal import Decimal

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone
import calendar

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

import smtplib
from email.mime.text import MIMEText

import redis

import settings 
import logging
from pprint import pprint, pformat

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

couch_server = couchdb.Server(settings.COUCH_DSN)
db_rssc = couch_server['rssc']
db_rssc_time_records = couch_server['rssc_time_records']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')

ADMIN_ID_RSSC = 159


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


@task(ignore_result=True)
def set_force_timeout(email):
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc']
    couchdb_time_records = s['rssc_time_records']
    doc = db.get(email)

    #check if doc is valid
    if doc == None:
        raise Exception('set_force_timeout %s' % email, 'Document does not exist!')

    #check if staff is still working
    if doc['status'] == 'not working':
        raise Exception('set_force_timeout %s' % email, 'Staff is not working anymore!')

    #check if staff does still have working_details on the document
    if doc.has_key('working_details') == False:
        raise Exception('set_force_timeout %s' % email, 'Staff has no working details!')

    if doc['status'] == 'working':
        a = doc['working_details']['time_in']
    elif doc['status'] == 'lunch break':
        a = doc['lunch_started']
    elif doc['status'] == 'quick break':
        a = doc['quick_break']

    prev_datetime = datetime(a[0], a[1], a[2], a[3], a[4], a[5])

    datetime_force_logout = get_ph_time()

    if datetime_force_logout <= prev_datetime:
        raise Exception('Force logout date/time for $s must be after %s' % (email, prev_datetime.strftime('%Y-%m-%d %H:%M')))

    #get client_name
    client_name = '%s %s' % (doc['working_details']['client_fname'], doc['working_details']['client_lname'])
    subcontractors_id = doc['working_details']['subcontract']


    #close all timerecords
    dt_out = [datetime_force_logout.year, 
            datetime_force_logout.month, datetime_force_logout.day, 
            datetime_force_logout.hour, datetime_force_logout.minute, 
            datetime_force_logout.second]

    if doc['status'] == 'quick break':
        doc_quick_break_id = doc.pop('quick_break_doc_id')
        doc_quickbreak = couchdb_time_records.get(doc_quick_break_id)
        doc_quickbreak['end'] = dt_out
        couchdb_time_records.save(doc_quickbreak)

    if doc['status'] == 'lunch break':
        doc_lunch_break_id = doc.pop('lunch_started_doc_id')
        doc_lunchbreak = couchdb_time_records.get(doc_lunch_break_id)
        doc_lunchbreak['end'] = dt_out
        couchdb_time_records.save(doc_lunchbreak)

    #set time_out on timerecord
    timerecord_id = doc['working_details']['timerecord']
    doc_timerecord = couchdb_time_records.get(timerecord_id)
    doc_timerecord['time_out'] = dt_out
    couchdb_time_records.save(doc_timerecord)

    #remove unnecessary details
    if doc.has_key('poll_connection'):
        doc.pop('poll_connection')
    if doc.has_key('notify_disconnected'):
        doc.pop('notify_disconnected')
    if doc.has_key('working_details'):
        doc.pop('working_details')
    if doc.has_key('sked'):
        doc.pop('sked')

    doc['status'] = 'not working'

    db.save(doc)

    #create an email doc
    now = datetime_force_logout
    doc_email = {'type': 'email', 'to': [doc['_id']]}
    doc_email['subject'] = 'RSSC Forced Timeout Notification due to clients depleted balance'
    doc_email['userid'] = doc['reference_id']
    doc_email['notify_type'] = 'rssc forced timeout'
    doc_email['generated'] = [now.year, now.month, now.day, 
        now.hour, now.minute, now.second]

    sql = text("""
        SELECT *
        FROM admin
        WHERE admin_id = :admin_id
    """)
    conn = engine.connect()
    admin = conn.execute(sql, admin_id=ADMIN_ID_RSSC).fetchone()

    doc_email['MIMEText'] = """Hi %s,\r\n\r\n%s %s has set your status to finish work for client %s.\r\nTimeout: %s Asia/Manila Timezone.\r\nReason: Clients Load Depleted\r\n\r\nPlease do not reply to this email.""" % (doc['fname'], admin.admin_fname, admin.admin_lname, client_name, datetime_force_logout.strftime('%Y-%m-%d %I:%M %p'))

    couchdb_reports = s['rssc_reports']
    couchdb_reports.save(doc_email)

    #create a note on TimesheetNotesAdmin

    #get the Timesheet first
    datetime_timesheet = datetime_force_logout.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    sql = text("""
        SELECT id 
        FROM timesheet
        WHERE subcontractors_id = :subcontractors_id
        AND month_year = :month_year
        AND status in ('open', 'suspended')
        """)
    timesheets = conn.execute(sql, month_year=datetime_timesheet, subcontractors_id=subcontractors_id).fetchall()

    for timesheet in timesheets:
        sql = text("""
            SELECT id
            FROM timesheet_details
            WHERE timesheet_id = :timesheet_id
            AND day = :day
        """)
        timesheet_details = conn.execute(sql, timesheet_id=timesheet[0], day=datetime_force_logout.day).fetchall()

        #get the TimesheetDetails
        for timesheet_detail in timesheet_details:
            #add note to the TimesheetNotesAdmin
            x = timesheet_detail[0]
            sql = """
                INSERT INTO timesheet_notes_admin
                (timesheet_details_id, admin_id, timestamp, note)
                VALUES (%s, %s, "%s", "%s")
            """ % (x, ADMIN_ID_RSSC, now.strftime('%F %H:%M:%S'), 'Forced Timeout: Clients Load Depleted')
            conn.execute(sql)

    conn.close()


@task
def is_old_client(client_id):
    """these are the clients before prepaid
    they have a -30 days_before_suspension field on their settings
    """
    #retrieve client settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']
    now_array = get_ph_time(as_array=True)
    view = db_client_docs.view('client/settings', startkey=[client_id, now_array],
        endkey=[client_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    doc_client_settings = view.rows[0].doc

    #dont include clients with -30 days
    if doc_client_settings.has_key('days_before_suspension'):
        if doc_client_settings['days_before_suspension'] == -30:
            return True

    return False


@task
def current_running_balance(client_id, doc_users, now):
    """
    returns current running balance
    """
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    r = db_client_docs.view('client/running_balance', key=client_id, group=True)
    
    if len(r.rows) == 0:
        running_balance = Decimal(0)
    else:
        running_balance = Decimal('%0.2f' % r.rows[0].value)

    for doc_user in doc_users:
        x = doc_user['working_details']['time_in']
        time_in = datetime(x[0], x[1], x[2], x[3], x[4], x[5])
        status = doc_user['status']
        if status == 'lunch break': #staff at lunch, no need to count lunch hours
            x = doc_user['lunch_started']
            datetime_lunch = datetime(x[0], x[1], x[2], x[3], x[4], x[5])
            dt = datetime_lunch - time_in
        else:
            dt = now - time_in

        #get total seconds
        total_seconds = (dt.days * 60 * 60 * 24) + dt.seconds
        hours = Decimal('%0.2f' % total_seconds) / Decimal('3600.0')
        client_price_hourly_rate = Decimal(doc_user['working_details']['client_price_hourly_rate'])
        amount = hours * client_price_hourly_rate

        running_balance -= amount

    return running_balance


@task(ignore_result=True)
def notify_csro(csro_email, doc_user):
    subject = 'FORCED TIMEOUT DUE TO DEPLETED LOAD BALANCE'
    if settings.DEBUG:
        subject = 'TEST %s' % subject

    if doc_user.has_key('working_details'):
        working_details = doc_user['working_details']
        client_fname = working_details['client_fname']
        client_lname = working_details['client_lname']
        job_designation = working_details['job_designation']
    else:
        client_fname = ''
        client_lname = ''
        job_designation = ''

    if doc_user.has_key('leads_id') == False:
        send_task("notify_devs.send", ("ALERT CANNOT FIND leads_id", "Cannot find leads_id from rssc document %s" % doc_user))
        leads_id = 'Missing'
    else:
        leads_id = doc_user['leads_id']

    message = """Staff %s %s, %s <userid:%s, email:%s, skype:%s>
for Client %s %s <client_id:%s>
was forced timeout while working due to clients depleted load balance.

Timestamp: %s
    """ % (doc_user['fname'], doc_user['lname'], job_designation, 
        doc_user['reference_id'], doc_user['_id'], doc_user['skype_id'],
        client_fname, client_lname, leads_id, get_ph_time())

    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = 'noreply@remotestaff.com.au'
    msg['To'] = csro_email

    s = smtplib.SMTP(host = settings.SMTP_CONFIG_SES['server'],
        port = settings.SMTP_CONFIG_SES['port'])
    s.starttls()
    s.login(settings.SMTP_CONFIG_SES['username'],
        settings.SMTP_CONFIG_SES['password']
        )

    recipients = [csro_email]
    if settings.DEBUG:
        recipients = []

    recipients.append('devs@remotestaff.com.au')    #TODO remove devs

    s.sendmail('noreply@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()



@task(ignore_result=True)
def run():
    redis_cursor = redis.StrictRedis()
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc']
    r = db.view('prepaid_monitoring/working_staff', include_docs=True)
    client_ids = {}
    for d in r:
        client_id = d['key']
        if client_ids.has_key(client_id) == False:
            client_ids[client_id] = []

        doc = d.doc
        client_ids[client_id].append(doc.copy())

    #retrieve client settings
    tasks = []
    for client_id in client_ids.keys():
        tasks.append(is_old_client.subtask((client_id,)))

    job = TaskSet(tasks=tasks)
    data = job.apply_async()
    old_client_result = data.join()
    map_result = map(None, client_ids.keys(), old_client_result)
    filter_result = filter(lambda x: x[1], map_result)
    client_ids_to_remove = map(lambda x: x[0],  filter_result)
    
    #remove old clients
    for client_id in client_ids_to_remove:
        client_ids.pop(client_id)

    #iterate on client_ids
    tasks = []
    db_client_docs = s['client_docs']
    now = get_ph_time()
    for client_id, doc_users in client_ids.iteritems():
        tasks.append(current_running_balance.subtask((client_id, doc_users, now)))

    job = TaskSet(tasks=tasks)
    data = job.apply_async()
    running_balance_list = data.join()
    map_running_balance_list = map(None, client_ids.iterkeys(), running_balance_list)
    filter_running_balance_list = filter(lambda x: x[1] < Decimal('0.00'), map_running_balance_list)
    for client_id, running_balance in filter_running_balance_list:
        logging.info('zero_balance client %s : %0.2f' % (client_id, running_balance))

        #send email to CSRO
        result = send_task("EmailSender.GetCsroEmail", [client_id,])
        csro_email = result.get()

        for doc_user in client_ids[client_id]:
            logging.info('zero_balance disconnect %s' % doc_user['_id'])
            if csro_email != None:
                notify_csro.delay(csro_email, doc_user)

            set_force_timeout.delay(doc_user['_id'],)
            redis_cursor.lpush('server_rssc:depleted_load_staff_emails', doc_user['_id'])


if __name__ == '__main__':
##~    print is_old_client(11)
##~    print is_old_client(58)
##~    print is_old_client(2287)
    send_task("prepaid_monitor_zero_balance.run", [])
