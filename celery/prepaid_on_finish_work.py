#   2013-12-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add excemption for rica and chris monitoring daily rate and suspension
#   2013-09-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add rate_limit on processsing time records
#   2013-08-16  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   Task #4486, When account reaches 5 POINT something days
#   2013-06-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3954, safety for disable_auto_follow_up = 'Y' when first invoice
#   2013-06-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed default exchange
#   2013-06-07  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added default exchange
#   2013-05-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add some proper rounding off decimal
#   -   consider manually generated invoice, and send them to client when client goes 5 days to 0
#   2013-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3807 restrict auto creation of invoice when there is a hourly rate change
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-29 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add a disable_auto_follow_up checking per invoice
#   2013-03-06 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   prevent raising errors on apply_gst and currency checks, just notify devs instead
#   2013-02-14 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   checks send_invoice_reminder setting
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the recorded client_hourly_rate from timerecords doc instead of querying the doc_subcontractor 
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed references on sqlalchemy
#   2013-01-17 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   prevent multiple charging timerecord by checking timerecord/tracking view from client_docs couchdb
#   2013-01-08 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3424, used prepaid_create_invoice.create celery task on creating invoice
#   -   removed create_invoice function from this script
#   2012-12-21 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   notify accounts for autodebit clients, wf 3419
#   2012-12-20 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add checking of autodebit=Y and prevent sending emails for
#       2days and 0days notice
#   2012-12-06 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add logging to monitor document conflicts
#   2012-12-04 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   send different email message, workflow 3330
#   -   bcc devs on all emails
#   2012-11-05 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   changed -18 days group to -30 days for old clients
#   2012-11-01 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   no resending of second day notice for -18 days group
#   2012-10-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bug fix on checking item length
#   2012-10-17 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix creation of invoice items, included suspended records
#   -   raise an exception if no items found
#   -   removed orders and cristina as default cc
#   2012-10-15 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix prevent checking second day notice if no invoice found
#   2012-10-10 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed total hours and hourly rate on item description
#   2012-10-01 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed auto generation of invoice for -18 days group
#   2012-09-28 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed checking of apply_gst from leads table
#   2012-09-26 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated added_by

import settings
import couchdb
import re
import string
from pprint import pformat

import pika
from celery.task import task, Task
from celery.execute import send_task
from celery import Celery

from Cheetah.Template import Template
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

import logging
from pymongo import MongoClient
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
import certifi

utc = timezone('UTC')
phtz = timezone('Asia/Manila')

WORKING_WEEKDAYS = 22
DAYS_BEFORE_INVOICE_ISSUE = 6
DAYS_INVOICE_OPEN = 365
TWOPLACES = Decimal(10) ** -2

import staff_daily_allowance


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

def add_worked_timerecord_with_date(doc_time_record_id, now, days_before_suspension):
    """generates a charge couchdb document and returns a copy of document
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_time_records = s['rssc_time_records']

    #get timerecord doc
    doc_time_record = db_time_records.get(doc_time_record_id)
    if doc_time_record == None:
        raise Exception ('ERROR PREPAID TIMERECORD',
            'Please check time_record doc_id %s' % doc_time_record_id)

    #check if timerecord is already charged
    if doc_time_record.has_key('doc_charge'):
        raise Exception('%s is already charged to %s' % (doc_time_record_id, doc_time_record['doc_charge']))

    #check if prepaid
    db = s['rssc']
    doc_subcon = db.get('subcon-%s' % doc_time_record['subcontractors_id'])

    if doc_subcon.has_key('prepaid') == False:
        raise Exception ('ERROR PREPAID TIMERECORD missing prepaid key',
            'Please check time_record doc_id %s' % doc_time_record_id)

    prepaid = doc_subcon['prepaid']
    if prepaid == 'no':
        raise Exception ('ERROR PREPAID TIMERECORD not a prepaid subcontract',
            'Please check time_record doc_id %s' % doc_time_record_id)

    leads_id = doc_subcon['leads_id']

    #check if leads_id matches
    if doc_time_record['leads_id'] != leads_id:
        raise Exception ('Failed to credit on finish work, leads_id does not match!', 'Please check leads_id for rssc %s. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))

    #check if client has couchdb settings
    db_client_docs = s['client_docs']
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1)

    if len(r.rows) == 0:    #no client settings, alert devs
        raise Exception ('Failed to credit on finish work. Missing client settings!', 'Please check client settings for leads_id %s. Failed to credit doc rssc_time_records %s' % (leads_id, doc_time_record_id))

    currency, apply_gst = r.rows[0]['value']

    #check if timerecord is already charged
    r = db_client_docs.view('timerecord/tracking', key=doc_time_record_id)
    if len(r.rows) > 1:
        raise Exception('%s timerecord is already charged to client_docs %s' % (doc_time_record_id, r.rows[0]['id']))
    
    #apply_gst check
    if doc_subcon['client'].has_key('apply_gst') == False:
        send_task('notify_devs.send', ['apply_gst does not exists', 'Please check rssc %s. Missing apply_gst on doc_subcon rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id)])

    if apply_gst == 'Y' and doc_subcon['client']['apply_gst'] == 'no':
        send_task('notify_devs.send', ['inconsistent apply_gst', 'Please check rssc %s. Inconsistent apply_gst on doc_subcon, doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id)])

    if apply_gst == 'N' and doc_subcon['client']['apply_gst'] == 'yes':
        send_task('notify_devs.send', ['inconsistent apply_gst', 'Please check rssc %s. Inconsistent apply_gst on doc_subcon, doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id)])

    #currency check
    if doc_subcon.has_key('client_currency') == False:
        send_task('notify_devs.send', ['client_currency not found', 'Please check rssc %s. Missing client_currency field for doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id)])
        raise Exception ()

    if currency != doc_subcon['client_currency']:
        send_task('notify_devs.send', ['client_currency inconsistent', 'Please check rssc %s. Inconsistent client_currency field for doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id)])

    #check client_price_hourly_rate
    if doc_subcon.has_key('client_price_hourly_rate') == False:
        raise Exception ('Failed to credit on finish work', 'Please check rssc %s. Missing client_price_hourly_rate. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))

    #check userid consistency
    if doc_subcon['userid'] != doc_time_record['userid']:
        raise Exception ('Failed to credit on finish work', 'Please check rssc %s. Inconsistent userids. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))

    a = doc_time_record['time_in']
    time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5])

    b = doc_time_record['time_out']
    time_out = datetime(b[0], b[1], b[2], b[3], b[4], b[5])

    if time_out < time_in:
        raise Exception ('Failed to credit on finish work', 'Please check rssc %s. time_out is less thant time_in. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))

    total_time = time_out - time_in

    userid = doc_subcon['userid']
    r = db_time_records.view('summary/userid_lunch_start', 
        startkey=[userid, doc_time_record['time_in']], 
        endkey=[userid, doc_time_record['time_out']],
        include_docs=True)


    lunch_time = timedelta(0)

    for x in r:
        lunch_doc = x.doc
        a = lunch_doc['start']
        b = lunch_doc['end']
        start = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
        end = datetime(b[0], b[1], b[2], b[3], b[4], b[5])
        lunch_time += end - start

    total_time -= lunch_time
    total_time_decimal = Decimal('%0.2f' % (total_time.seconds / 3600.0)).quantize(TWOPLACES)
    r = db_client_docs.view('client/running_balance', key=leads_id)

    if len(r.rows) == 0:
        running_balance = Decimal('0.00').quantize(TWOPLACES)
    else:                                                               
        running_balance = Decimal('%s' % r.rows[0].value).quantize(TWOPLACES)

    #get hourly rate from timerecord
    if doc_time_record.has_key('client_hourly_rate') == False:
        send_task('notify_devs.send', ['client_hourly_rate not recorded', 'Please check : %r' % doc_time_record.copy()])
        t = send_task('subcontractors.get_hourly_rate', [doc_time_record['subcontractors_id'], time_in])
        client_hourly_rate = t.get()
    else:
        client_hourly_rate = Decimal(doc_time_record['client_hourly_rate'])

    amount = client_hourly_rate * total_time_decimal
    charge = Decimal(amount).quantize(TWOPLACES)

    particular = 'Staff %s %s, <%s> worked for %0.2f hours @ %0.2f/hr' % (
        doc_subcon['staff']['fname'], 
        doc_subcon['staff']['lname'], 
        doc_subcon['job_designation'], 
        total_time_decimal, 
        client_hourly_rate,
        )

    #apply_gst
    if apply_gst == 'Y':
        charge += charge * Decimal('0.1').quantize(TWOPLACES)
        particular += ' plus GST'

    running_balance -= charge

    doc_transaction = dict(
        added_by = 'RSSC Prepaid On Finish Work',
        added_on = now,
        charge = '%0.2f' % charge,
        client_id = leads_id,                                          
        credit = '0.00',
        credit_type = 'WORK',
        currency = currency,
        remarks = 'Generated from time sheet',
        type = 'credit accounting',
        running_balance = '%0.2f' % running_balance,
        particular = particular,
        doc_subcon = doc_subcon['_id'],
        doc_time_record_id = doc_time_record_id,
        client_hourly_rate = '%0.2f' % client_hourly_rate,
    )
    
    #deduct currency adjustment
    db_client_docs.save(doc_transaction)
    if days_before_suspension not in [ Decimal('-30.0') ]:
        deduct_currency_adjustment(doc_time_record_id,  doc_time_record['subcontractors_id'])
    
    
    
    return doc_transaction.copy()


def has_open_invoice(leads_id, days):
    """check for open invoice for the past x days
    returns None else, returns a copy of the document
    just for checking the second_day notice
    """
    now = get_ph_time(as_array = False)
    prev_date = now - timedelta(days=days)

    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    r = db.view('client/mq_created_order', 
        startkey=[leads_id, 
            [prev_date.year, 
                prev_date.month, 
                prev_date.day, 
                prev_date.hour, 
                prev_date.minute, 
                prev_date.second, 
                0]],
        endkey=[leads_id,
            [now.year,
                now.month,
                now.day,
                now.hour,
                now.minute,
                now.second,
                0]],
        include_docs=True)

    if len(r.rows) == 0:
        return None
    else:
        return r.rows[0].doc.copy()


def send_0_to_negative_day_notice(doc_id):
    """given the doc_id, send the 0 day negative notice
    """
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('FAILED to set 0 to negative day notice', 'Please check doc_id:%s.' % (doc_id))

    if doc.has_key('type') == False:
        raise Exception('FAILED to set 0 to negative day notice', 'Please check doc_id:%s. missing type' % (doc_id))

    if doc.has_key('zero_to_negative_day_notice'):
        logging.info('zero_to_negative_day_notice already sent for %s' % doc_id)
        return

    i = 0
    while True:
        try:
            doc['zero_to_negative_day_notice'] = get_ph_time().strftime('%F %H:%M:%S')
            db.save(doc)
            break
        except:
            i = i + 1
            if i == 10:
                break
            doc = db.get(doc_id)
            
    logging.info('zero_to_negative_day_notice set to %s' % doc_id)

    #send task to send a different note
    #get client emails
    import sc_celeryconfig
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    result = celery.send_task("EmailSender.GetEmails", [doc['client_id']])
    email_recipients = result.get()
    email_to_list = email_recipients['recipients']

    #default cc list
    email_cc_list = []

    #add the assigned csro email
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    result_csro_email = celery.send_task("EmailSender.GetCsroEmail", [doc['client_id']])
    csro_email = result_csro_email.get()
    if csro_email != None:
        email_cc_list.append(csro_email)

    #bcc devs
    email_bcc_list = ['devs@remotestaff.com.au']
    email_from = 'accounts@remotestaff.com.au'

    import prepaid_send_invoice
    prepaid_send_invoice.send_0_to_negative(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from)


def set_second_day_notice(doc_id):
    """given the doc_id, add field second_day_notice
    """
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('FAILED to set second_day_notice', 'Please check doc_id:%s.' % (doc_id))

    if doc.has_key('type') == False:
        raise Exception('FAILED to set second_day_notice', 'Please check doc_id:%s. missing type' % (doc_id))

    doc['second_day_notice'] = get_ph_time().strftime('%F %H:%M:%S')
    db.save(doc)
    logging.info('second_day_notice set to %s, rev %s' % (doc_id, doc['_rev']))

    #send task to send a different note
    #get client emails
    import sc_celeryconfig
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    result = celery.send_task("EmailSender.GetEmails", [doc['client_id']])
    email_recipients = result.get()
    email_to_list = email_recipients['recipients']

    #default cc list
    email_cc_list = []

    #add the assigned csro email
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    result_csro_email = celery.send_task("EmailSender.GetCsroEmail", [doc['client_id']])
    csro_email = result_csro_email.get()
    if csro_email != None:
        email_cc_list.append(csro_email)

    #bcc devs
    email_bcc_list = ['devs@remotestaff.com.au']
    email_from = 'accounts@remotestaff.com.au'

    import prepaid_send_invoice
    prepaid_send_invoice.send_2_days_to_0(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from)


def add_week_days(reference_date, days):
    """add week days given the reference_date
    """
    #step a day back to include the reference date
    return_date = reference_date - timedelta(days = 1)

    #start looping
    i = 0
    while i < days:
        return_date = return_date + timedelta(days = 1)
        if return_date.strftime('%a') in ['Sat', 'Sun']:
            continue
        i += 1

    return return_date


def send_email(doc_id):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to send email.', 'Document %s not found.' % doc_id)

    #get client emails
    import sc_celeryconfig
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    result = celery.send_task("EmailSender.GetEmails", [doc['client_id']])
    email_recipients = result.get()
    email_to_list = email_recipients['recipients']

    #default cc list
    email_cc_list = []

    #add the assigned csro email
    celery = Celery()
    celery.config_from_object(sc_celeryconfig)
    
    result_csro_email = celery.send_task("EmailSender.GetCsroEmail", [doc['client_id']])
    csro_email = result_csro_email.get()
    if csro_email != None:
        email_cc_list.append(csro_email)

    #bcc devs
    email_bcc_list = ['devs@remotestaff.com.au']
    email_from = 'accounts@remotestaff.com.au'

    #send celery task
    import prepaid_send_invoice
    prepaid_send_invoice.send(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from)

    return 'ok'


@task(ignore_result=True)
def process_time_record(doc_id):
    process_time_record_with_date(doc_id)
    return

@task(ignore_result=True)
def process_time_record_with_date(doc_id):
    logging.info('checking %s' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']
    doc = db.get(doc_id)
    
    
    
    if doc == None:
        raise Exception('timerecord document not found', doc_id)
  
    #check if time_out has a value
    if not doc.has_key("time_out"):
        logging.info("Missing time_out key for rssc_time_record %s " % doc_id)
        return
      
    if doc.has_key("time_out") and doc["time_out"]==None:
        logging.info("Timeout value for rssc_time_record %s is NONE " % doc_id)
        return
  
  
    #check if prepaid
    db = s['rssc']
    doc_subcontractor = db.get('subcon-%s' % doc['subcontractors_id'])
    if 'prepaid' in doc_subcontractor == False:
        return
    if doc_subcontractor['prepaid'] != 'yes':
        return
  
    #get client settings
    db = s['client_docs']
    leads_id = doc['leads_id']
    now = get_ph_time(as_array=True)
    view = db.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)
    doc_client_settings = view.rows[0].doc
  
    if doc_client_settings.has_key('days_before_suspension'):
        days_before_suspension = Decimal('%s' % doc_client_settings['days_before_suspension'])
    else:
        days_before_suspension = Decimal('0')
  
    if doc_client_settings.has_key('days_before_invoice'):
        days_before_invoice = Decimal('%s' % doc_client_settings['days_before_invoice'])
    else:
        days_before_invoice = Decimal('%s' % DAYS_BEFORE_INVOICE_ISSUE)
  
    if doc_client_settings.has_key('days_to_invoice'):
        days_to_invoice = doc_client_settings['days_to_invoice']
    else:
        days_to_invoice = WORKING_WEEKDAYS
  
    autodebit = 'N'
    if doc_client_settings.has_key('autodebit'):
        if doc_client_settings['autodebit'] == 'Y':
            autodebit = 'Y'
  
    send_invoice_reminder = 'Y'
    if doc_client_settings.has_key('send_invoice_reminder'):
        send_invoice_reminder = doc_client_settings['send_invoice_reminder']
  
  
          
    check_if_logged_in = staff_daily_allowance.check_if_already_logged_in(doc)
    #print check_if_logged_in
    
    email_subcon = False
    to_be_charged = True
       
    if not check_if_logged_in['has_logged_in']:
           
        if check_if_logged_in['total_time_decimal'] <= check_if_logged_in['work_hours'] :
            logging.info("logged in hours %s <= contract allowed hours %s " % (check_if_logged_in['total_time_decimal'], check_if_logged_in['work_hours'])) 
            logging.info("staff_daily_allowance = %s" % check_if_logged_in['total_time_decimal'])    
           
            staff_daily_allowance.insert_rssc_staff_daily_allowance(check_if_logged_in['total_time_decimal'], check_if_logged_in['total_time_decimal'] , check_if_logged_in['client_perspective_time_in'], doc )
           
        else:
            email_subcon = True            
            logging.info("staff_daily_allowance = %s" % check_if_logged_in['work_hours'])    
               
            staff_daily_allowance.insert_rssc_staff_daily_allowance(
                check_if_logged_in['work_hours'], 
                check_if_logged_in['work_hours'] , 
                check_if_logged_in['client_perspective_time_in'],
                doc
            )
    else:
           
        #summation of staff daily allowance
           
        logging.info("has_logged_in = %s" % check_if_logged_in['has_logged_in'])
        logging.info("sum = %s" % check_if_logged_in['sum'])
        sum = check_if_logged_in['sum']
           
        if (check_if_logged_in['total_time_decimal'] + sum) <= check_if_logged_in['work_hours']:
               
            logging.info(" %s <= %s " % ((check_if_logged_in['total_time_decimal'] + sum), check_if_logged_in['work_hours']))
            logging.info("insert into staff daily allowance == logged in hours")
            staff_daily_allowance.insert_rssc_staff_daily_allowance((check_if_logged_in['total_time_decimal'] + sum), check_if_logged_in['total_time_decimal'] , check_if_logged_in['client_perspective_time_in'], doc )
               
        else:
               
            if (check_if_logged_in['work_hours'] - sum) != 0 :
                logging.info(" %s - %s " % ((check_if_logged_in['work_hours'] - sum), check_if_logged_in['work_hours']))    
                   
                email_subcon = True
                staff_daily_allowance.insert_rssc_staff_daily_allowance(
                    check_if_logged_in['work_hours'], 
                    (check_if_logged_in['work_hours'] - sum) , 
                    check_if_logged_in['client_perspective_time_in'], 
                    doc 
                )
            else:
                logging.info("Exceeded working hours . Allowed worked hours is : %s . Total recorded hours : %s. Attempted worked hours : %s" % (check_if_logged_in['work_hours'], sum, (check_if_logged_in['total_time_decimal'] + sum) ) )    
                email_subcon = True  
                to_be_charged = False
                   
                   
    logging.info("email_subcon : %s "  % email_subcon)     
    if to_be_charged:
        doc_transaction = staff_daily_allowance.add_worked_timerecord_with_capping(doc_id, doc["time_out"], days_before_suspension)
        adjust_timesheet = staff_daily_allowance.adjust_timesheet(doc)
           
                    
    if email_subcon:
        logging.info("Send an email to staff for overtime notification")
        staff_daily_allowance.send_overtime_notification(doc_id)
               
    #Get latest running balance
    r = db.view('client/running_balance', key=leads_id)
    if len(r.rows) == 0:
        running_balance = Decimal('0.00').quantize(TWOPLACES)
    else:                                                               
        running_balance = Decimal('%s' % r.rows[0].value).quantize(TWOPLACES)
     
    logging.info("latest running_balance : %s " % running_balance)
     
    #try direct import instead asynchronous approach
    try:
        logging.info("Trying to get Clients Daily Rate %s " % leads_id)
        import ClientsWithPrepaidAccounts
        clients_daily_rate = ClientsWithPrepaidAccounts.get_clients_daily_rate(leads_id)
        #clients_daily_rate = r.get()    
    except:
        logging.info("Failed to get Clients Daily Rate %s trying normal approach " % leads_id)
        import ClientsWithPrepaidAccounts
        clients_daily_rate = ClientsWithPrepaidAccounts.get_clients_daily_rate(leads_id)
  
      
    #fun starts here
    if clients_daily_rate == Decimal('0.00'):   #prevent division by zero
        if leads_id in [11, 7935]: #chris and rica are excempted
            return
        else:
            logging.info('leads_id : %s has daily_rate of ZERO, probably from a terminated contract while working.' % (leads_id))
            send_task('notify_devs.send', ['leads_id : %s has daily_rate of ZERO' % leads_id, 'timerecord related: rssc_time_records %s' % doc_id])
            return
      
      
    logging.info("clients_daily_rate %s " % clients_daily_rate)
      
    logging.info("running_balance %s " % running_balance)
      
    logging.info("days_before_invoice %s " % days_before_invoice)
              
    if (running_balance / clients_daily_rate) < days_before_invoice:
        doc_open_invoice = has_open_invoice(leads_id, DAYS_INVOICE_OPEN)
        flag_sent_auto_responder_to_csro = False
        sent_flag = None

        if days_before_suspension != Decimal('-30.0'): #no generation of invoice for the -30 days group
            if doc_open_invoice == None:    #no open invoice for the past DAYS_INVOICE_OPEN, create one
                logging.info("Creating prepaid invoice for leads_id %s " % leads_id)
                import prepaid_create_invoice
                doc_open_invoice = prepaid_create_invoice.create_invoice(leads_id, days_to_invoice, False, 'celery prepaid_on_finish_work')
                #t = send_task('prepaid_create_invoice.create_invoice', [leads_id, days_to_invoice, False, 'celery prepaid_on_finish_work']) #special cases like #5427 daniel cowart
                #doc_open_invoice = t.get()
  
                if doc_open_invoice == None:
                    raise Exception('Failed to create invoice', 'leads_id : %s, rssc_time_records doc_id : %s' % (leads_id, doc_id))
  
                send_email(doc_open_invoice['_id'])
                sent_flag = 'Y'
  
                #notify accounts, wf 3419
                if autodebit == 'Y':
                    send_task('prepaid_send_invoice.notify_accounts_autodebit', [doc_open_invoice,])
  
                #celery task to send autoresponder to csro
                send_task("PrepaidCSROAutoResponders.ClientRunningLowOnCredit", [doc_open_invoice['_id']])
                flag_sent_auto_responder_to_csro = True
            else:
                sent_flag = doc_open_invoice.get('sent_flag')

            disable_auto_follow_up = 'N'
            if doc_open_invoice.has_key('disable_auto_follow_up'):
                disable_auto_follow_up = doc_open_invoice['disable_auto_follow_up']

            #if invoice is first, disable_auto_follow_up="Y"
            order_id = doc_open_invoice.get('order_id')
            leads_id_tmp, series = re.split('-', order_id)
            series = int(series)
            if series == 1:
                disable_auto_follow_up = 'Y'

            #check if invoice is manually generated, send it if not sent yet
            if disable_auto_follow_up == 'N' and sent_flag != 'Y':
                send_email(doc_open_invoice['_id'])

            if (doc_open_invoice != None) and ((running_balance / clients_daily_rate) < Decimal('2.0')): #second day notice
                if doc_open_invoice.has_key('second_day_notice') == False:
                    if send_invoice_reminder == 'Y':
                        if autodebit == 'N':
                            if disable_auto_follow_up != 'Y':   #more fine grained checking of sending invoice follow up
                                set_second_day_notice(doc_open_invoice['_id'])
                            else:
                                send_task('skype_messaging.notify_skype_id', ['ACK on disable_auto_follow_up second day notice.\nInvoice related: %s' % doc_open_invoice['order_id'], 'locsunglao'])

                    if flag_sent_auto_responder_to_csro == False:
                        send_task("PrepaidCSROAutoResponders.ClientRunningLowOnCredit", [doc_open_invoice['_id']])

            if running_balance <= Decimal('0.00'):
                if send_invoice_reminder == 'Y':
                    if autodebit == 'N':
                        send_0_to_negative_day_notice(doc_open_invoice['_id'])
                    else:
                        send_task('skype_messaging.notify_skype_id', ['ACK on disable_auto_follow_up send_0_to_negative_day_notice day notice.\nInvoice related: %s' % doc_open_invoice['order_id'], 'locsunglao'])


    #suspension related
    if (running_balance / clients_daily_rate) <= days_before_suspension:
        if leads_id in [11, 7935]: #chris and rica are excempted
            return
          
        #if payment advise has been received, do not suspend
        try:
            from StringIO import StringIO
            import pycurl
              
            url = settings.API_URL+"/invoices/has-payment-advise/?client_id="+str(leads_id)
              
            storage = StringIO()
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.WRITEFUNCTION, storage.write)
            c.setopt(pycurl.SSL_VERIFYPEER, 1)
            c.setopt(pycurl.SSL_VERIFYHOST, 2)
            c.setopt(pycurl.CAINFO, certifi.where())
            c.perform()
            c.close()
            content = storage.getvalue()
            import json
            content = json.loads(content)
            if content["result"]:
                return
        except:
            pass
          
        if days_before_suspension not in [ Decimal('-30.0') ]:
      
            logging.info("suspending via celery_suspension client_id:%s running_balance:%s clients_daily_rate:%s days_before_suspension:%s" % (leads_id, running_balance, clients_daily_rate, days_before_suspension))
            import celery_suspension_config         
            celery = Celery()
            celery.config_from_object(celery_suspension_config)
            logging.info("suspending via celery_suspension client_id:%s running_balance:%s clients_daily_rate:%s days_before_suspension:%s" % (leads_id, running_balance, clients_daily_rate, days_before_suspension))
            celery.send_task("suspend.process", [leads_id])                        
    
    

def deduct_currency_adjustment(doc_id, subcon_id):  
    
    logging.info("deducting currency adjustment %s timerecord %s subcon_id" % (doc_id, subcon_id))   
    from StringIO import StringIO
    import pycurl
    
    url = settings.API_URL+"/currency-adjustment/deduct-mongo-to-running-balance/?doc_id="+str(doc_id)+"&subcon_id="+str(subcon_id)
    
    storage = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.WRITEFUNCTION, storage.write)
    c.setopt(pycurl.SSL_VERIFYPEER, 1)
    c.setopt(pycurl.SSL_VERIFYHOST, 2)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.perform()
    c.close()
    
        


if __name__ == '__main__':
    logging.info('tests')
    process_time_record('a6f0f27e806e764f5074e0d658f02395')
##~    process_time_record('FAIL RECORD')
##~    process_time_record('a6fc1d41cff1037ffb0852d6a4b77205')
##~    process_time_record('02e46add5def9fcf07de0ae8f25894ae')
##~    process_time_record('4d6a37a1aeaaf01836f605942f66ef4a') #daniel cowart
##~    logging.info(has_open_invoice(5427, 5))    #daniel cowart
##~    logging.info(has_open_invoice(8317, 15))
##~    logging.info(set_second_day_notice('FAIL invoice'))
##~    logging.info(set_second_day_notice('07bf99b71b5b3f81030965719929a900'))

##~    logging.info(send_email('07bf99b71b5b3f8103096571998d6d0c'))
##~    logging.info(send_0_to_negative_day_notice('07bf99b71b5b3f8103096571998d6d0c'))
