# 2017-05-11 Normaneil E. Macutay <normaneil.macutay@gmail.com>


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
from decimal import Decimal, ROUND_HALF_UP

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

import logging
import MySQLdb
from pymongo import MongoClient
from bson.objectid import ObjectId
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

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

def safe_mongocall(call):
    def _safe_mongocall(*args, **kwargs):
        for i in xrange(5):
            try:
                return call(*args, **kwargs)
            except pymongo.AutoReconnect:
                time.sleep(pow(2, i))
        print 'Error: Failed operation!'
    return _safe_mongocall


@safe_mongocall
def insert_to_collection(col, record):
    col.insert(record)

@safe_mongocall
def find_one(col, params):
    return col.find_one(params)

@safe_mongocall
def find_all(col, params):
    return col.find(params)
    
def insert_rssc_staff_daily_allowance(staff_daily_allowance, work_hours, client_perspective_time_in, doc_time_record):
    subcontractors_id = doc_time_record['subcontractors_id']
    
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017) 
                    
    db = mongo_client.prod
    col = db.rssc_staff_daily_allowance
    
    
    mongo_doc=find_one(col, {'couch_id' : doc_time_record['_id']})
    
    
    
    phtz = timezone('Asia/Manila')
    a = doc_time_record['time_in']
    staff_perspective_time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=phtz)
    if mongo_doc == None:                
        record={            
            'subcontractors_id' : subcontractors_id,    
            'staff_daily_allowance' :'%s' % staff_daily_allowance,             
            'work_hours' : '%s' % work_hours,
            'client_perspective_time_in' : client_perspective_time_in,
            'client_reference_date' : client_perspective_time_in.strftime('%Y-%m-%d'),
            'staff_perspective_time_in' : staff_perspective_time_in,
            'staff_reference_date' : staff_perspective_time_in.strftime('%Y-%m-%d'),
            'couch_id' : doc_time_record['_id']        
        }
        #col.insert(record)
        insert_to_collection(col, record)
        
    else:
       raise Exception ("Already exist in rssc_staff_daily_allowance for couch_id : %s" % doc_time_record['_id'])
    
    
def send_overtime_notification(doc_time_record_id):
    
    database = MySQLdb.connect(**settings.DB_ARGS)
       
    s = couchdb.Server(settings.COUCH_DSN)
    db_time_records = s['rssc_time_records']
    db_client_docs = s['client_docs']
    
    #get timerecord doc
    doc_time_record = db_time_records.get(doc_time_record_id)
    #logging.info(doc_time_record)
    
    subcontractors_id = doc_time_record['subcontractors_id']
    leads_id = doc_time_record['leads_id']
    userid = doc_time_record['userid']
    #check if prepaid
    db = s['rssc']
    doc_subcon = db.get('subcon-%s' % subcontractors_id)
    #logging.info(doc_subcon)
    
    
    a = doc_time_record['time_in']
    reference_date = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
    
    reference_start_date_key = [a[0], a[1], a[2], 0, 0, 0]
    reference_end_date_key = [a[0], a[1], a[2], 23, 59, 59]
    
    #Get staff work_status
    conn = database.cursor()
    sql = "SELECT work_status, client_timezone, staff_working_timezone FROM subcontractors WHERE id=%s;" %  subcontractors_id 
    subcon = conn.execute(sql)    
    subcon = dictfetchall(conn)
    work_status = subcon[0]['work_status']
    client_timezone = subcon[0]['client_timezone']
    staff_working_timezone = subcon[0]['staff_working_timezone']
    conn.close()
    
    conn = database.cursor()
    sql = "SELECT a.admin_fname, a.admin_lname, a.admin_email FROM leads l JOIN admin a ON a.admin_id = l.csro_id WHERE l.id=%s;" %  leads_id
    subcon = conn.execute(sql)    
    csro = dictfetchall(conn)
    csro_name = "%s %s" % ( csro[0]['admin_fname'], csro[0]['admin_lname'] )
    csro_email = "%s" % csro[0]['admin_email']
    conn.close()
    
    
    #Get all recorded logged hours
    result = get_total_logged_hrs(doc_time_record, client_timezone)  
    #print result  
    staff_perspective_reference_date = result['staff_perspective_reference_date']
    client_perspective_reference_date = result['client_perspective_reference_date']    
    total_logged_hrs = result['total_logged_hrs']
    timerecords = result['timerecords']
    #logging.info(result)
    
    #Get Lunch Records
    r = db_time_records.view('summary/userid_lunch_start', 
        startkey=[userid, reference_start_date_key], 
        endkey=[userid, reference_end_date_key],
        include_docs=True)


    lunch_time = timedelta(0)

    for x in r:
        lunch_doc = x.doc
        a = lunch_doc['start']
        b = lunch_doc['end']
        start = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
        end = datetime(b[0], b[1], b[2], b[3], b[4], b[5])
        lunch_time += end - start
        
    lunch_time_decimal = Decimal('%0.2f' % (lunch_time.seconds / 3600.0)).quantize(TWOPLACES)     
    logging.info("lunch_time_decimal : %s" % lunch_time_decimal)
    
          
    #Calculate total hours to be charged 
    result_charge = get_total_charged_hrs(work_status, total_logged_hrs);
    total_time_decimal = result_charge['total_time_charge']
    extra_hours = result_charge['extra_hours']
    total_logged_hrs = result_charge['total_logged_hrs']
    
    total_logged_hrs -= lunch_time_decimal
    
    logging.info("total_logged_hrs : %s" % total_logged_hrs)
    
    #Total hours to be charge  
    msg = ""  
    for timerecord in timerecords:
        time_in = timerecord['time_in']
        time_out = timerecord['time_out']
        #logging.info("Time In %s - Time Out %s " % (time_in.strftime('%Y-%m-%d %H:%M:%S'), time_out.strftime('%Y-%m-%d %H:%M:%S') ))
        msg += "<br>Time In %s - Time Out %s " % (time_in.strftime('%Y-%m-%d %H:%M:%S'), time_out.strftime('%Y-%m-%d %H:%M:%S') )
         
    #logging.info('total_time_decimal :  %s' % total_time_decimal)      
    #logging.info('extra_hours :  %s' % extra_hours)
    #logging.info('total_logged_hrs :  %s' % total_logged_hrs)
     
    #Send Overtime Notification
    subject = "Overtime for staff %s %s - %s" % (doc_subcon['staff']['fname'], doc_subcon['staff']['lname'], reference_date.strftime('%Y-%m-%d')) 
    message = """Dear %s %s, <br><br>
 
 You have exceeded the allowable worked hours for the day as per contract with client %s %s <br>
 <br>
 More Details about this overtime below. <br>
 
 %s
 
 <br>
 <br>
 Contract Hours : %s <br>
 Extra Hours : %s <br>
 Total Logged Hours : %s <br> 
 
 <br>
 For overtime approval, please contact your client. If it has been approved, 
 please contact compliance team at attendance@remotestaff.com.au or provide a note with screenshot in this page:
 <br>
 <br>
 https://remotestaff.com.au/portal/django/staff_productivity_reports/timesheets
 <br>
 <br>
 <br>
 This is a system generated e-mail. For any questions please connect with your assigned Staffing Consultant. Thank you.
 <br>
 <br>
 <br>
 <div style='color:#CCC'><small>%s</small></div>
 """ % (
       doc_subcon['staff']['fname'], 
       doc_subcon['staff']['lname'], 
       doc_subcon['client']['fname'],
       doc_subcon['client']['lname'],
       str(msg),
       total_time_decimal,
       extra_hours,
       total_logged_hrs,
       doc_time_record_id
       )
  
    to=[]    
    to.append(doc_subcon['staff']['email'])
    to.append(doc_subcon['staff']['registered_email'])
    #to.append(csro_email)
    to.append("attendance@remotestaff.com.au")
    doc = dict(
        to = to,
        bcc = None,
        created = get_ph_time(as_array=True),
        generated_by = 'prepaid_onf_finish_work.send_overtime_notification',
        text = None,
        html = message,
        subject = subject,
        sent = False,
     )
    
    doc['from'] = "Attendance<attendance@remotestaff.com.au>"      
    #logging.info(doc)
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['mailbox']
    db.save(doc)
    logging.info("Mailbox doc created")

    
def check_if_already_logged_in(doc_time_record):
    logging.info("Checking staff daily allowance")
    
    s = couchdb.Server(settings.COUCH_DSN)
    database = MySQLdb.connect(**settings.DB_ARGS)
    conn = database.cursor()
       
    subcontractors_id = doc_time_record['subcontractors_id']
    userid = doc_time_record['userid']
    
    #Get staff work_status
    sql = "SELECT work_status, client_timezone, staff_working_timezone FROM subcontractors WHERE id=%s;" %  subcontractors_id 
    subcon = conn.execute(sql)    
    subcon = dictfetchall(conn)
    work_status = subcon[0]['work_status']
    client_timezone = subcon[0]['client_timezone']
    staff_working_timezone = subcon[0]['staff_working_timezone']    
    conn.close()
    
    phtz = timezone('Asia/Manila')
    
    if work_status == 'Part-Time':
        work_hours =4
    else:
        work_hours =8   
         
    work_hours = Decimal(work_hours).quantize(TWOPLACES)     
    #client timezone
    timezone_ref = client_timezone
    client_timezone = timezone(timezone_ref)
    
    a = doc_time_record['time_in']
    b = doc_time_record['time_out']
    time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
    time_out = datetime(b[0], b[1], b[2], b[3], b[4], b[5])
    
    if time_out < time_in:
        raise Exception ('Failed to credit on finish work', 'Please check rssc_time_records %s. time_out is less thant time_in' % doc_time_record['_id'])
    
    total_time = time_out - time_in
    
    
    db_time_records = s['rssc_time_records']        
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
    lunch_time_decimal = Decimal('%0.2f' % (lunch_time.seconds / 3600.0)).quantize(TWOPLACES)
    logging.info("total_time_decimal : %s " % total_time_decimal)
    logging.info("lunch_time_decimal : %s " % lunch_time_decimal)
        
    #convert time_in client timezone
    reference_start_date = datetime(a[0], a[1], a[2], 0, 0, 0, tzinfo=client_timezone)    
    reference_end_date = datetime(a[0], a[1], a[2], 23, 59, 59, tzinfo=client_timezone)
    client_perspective_time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=client_timezone)
        
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017) 
                    
    db = mongo_client.prod
    col = db.rssc_staff_daily_allowance
    try:
        cursor = find_all(col, {'subcontractors_id' : subcontractors_id, 'client_perspective_time_in' : {"$gt" : reference_start_date, "$lt" : reference_end_date}})        
    except:
        logging.info("No documents found")
        
    logging.info("cursor.count : %s" % cursor.count())
    has_logged_in =  False
    sum = 0
    if cursor.count() > 0 :
        has_logged_in =  True
                
        for record in cursor:
            logging.info("_id : %s" %  (record['_id'])) 
            sum += float(record['work_hours'])
            
        
       
    sum = Decimal('%s' % sum).quantize(TWOPLACES)
    return dict(
        has_logged_in = has_logged_in,
        total_time_decimal = total_time_decimal,
        lunch_time_decimal = lunch_time_decimal,
        work_hours = work_hours,
        client_perspective_time_in = client_perspective_time_in,  
        sum = sum           
    )
    
            
def get_total_logged_hrs(doc_time_record, client_timezone):
    
    s = couchdb.Server(settings.COUCH_DSN)
    db_time_records = s['rssc_time_records']
    
    doc_time_record_id = doc_time_record['_id']
    type = doc_time_record['type']
    
    if type == "timerecord":
         
        if doc_time_record['time_out'] == False:
            raise Exception ('ERROR TIMERECORD TIME_OUT NULL VALUE',
                'Please check time_record doc_id %s' % doc_time_record_id)
     
        userid = int(doc_time_record['userid'])
        subcontractors_id = int(doc_time_record['subcontractors_id'])
        #staff default timezone
        phtz = timezone('Asia/Manila')
         
        #client timezone
        timezone_ref = client_timezone
        client_timezone = timezone(timezone_ref)
         
        a = doc_time_record['time_in']
        b = doc_time_record['time_out']
        time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=phtz)    
        time_out = datetime(b[0], b[1], b[2], b[3], b[4], b[5], tzinfo=phtz)
        staff_perspective_reference_date = datetime(a[0], a[1], a[2], 0, 0, 0, tzinfo=phtz)
        
        
        #convert in client timezone
        client_perspective_time_in = time_in.astimezone(client_timezone)
        client_perspective_time_out = time_out.astimezone(client_timezone)
         
        reference_start_date_key = [a[0], a[1], a[2], 0, 0, 0]
        reference_end_date_key = [a[0], a[1], a[2], 23, 59, 59]
         
        reference_start_date = datetime(a[0], a[1], a[2], 0, 0, 0, tzinfo=phtz)    
        reference_end_date = datetime(a[0], a[1], a[2], 23, 59, 59, tzinfo=phtz)
        client_perspective_reference_date = datetime(a[0], a[1], a[2], 0, 0, 0, tzinfo=client_timezone)
        
        timerecords = db_time_records.view('prepaid/timerecords', startkey=[userid, subcontractors_id, reference_start_date_key], endkey=[userid, subcontractors_id, reference_end_date_key]).rows
        #logging.info(timerecords) 
        total_logged_hrs = Decimal(0)
        total_lunch_hours = Decimal(0)
        data_timerecords = []
        doc_time_record_ids = []
        for t in timerecords:
            
            record_type = t['value'][0]
            a = t['key'][2]
            start = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=phtz)
            logging.info("Time In : %s" % start)  
            
            b = t['value'][0]
            if b == None or b == False:
                end = None
            else:
                end = datetime(b[0], b[1], b[2], b[3], b[4], b[5], tzinfo=phtz)
            logging.info("Time Out : %s" % end)    
            
            if end != None:                
                #update totals
                time_diff = end - start
                logging.info("Time Diff : %s" % time_diff)
                time_diff_decimal = Decimal('%s' % ((time_diff.seconds + (time_diff.days*86400)) / 3600.0)).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
                total_logged_hrs += time_diff_decimal
                
                doc_time_record_ids.append("couch_id : %s" % t['id'])          
                data_timerecords.append(dict(
                    time_in = start,
                    time_out = end,
                    total_time_decimal = time_diff_decimal                        
                ))
        

                     
        #logging.info("total_logged_hrs : %s " % total_logged_hrs)
        total_logged_hrs = Decimal(total_logged_hrs).quantize(TWOPLACES)        
        return dict(
            total_logged_hrs = total_logged_hrs , 
            doc_time_record_ids = doc_time_record_ids, 
            staff_perspective_reference_date = staff_perspective_reference_date,
            client_perspective_reference_date = client_perspective_reference_date,
            timerecords = data_timerecords
        )
        
def get_total_charged_hrs(work_status, total_logged_hrs):
    #logging.info("work_status : %s" % work_status)
    if work_status == "Part-Time" :
        work_hours = 4.0
    else:
        work_hours = 8.0
            
    extra_hours = 0.0
    total_time_charge = 0.0    
    
    total_logged_hrs = Decimal(total_logged_hrs).quantize(TWOPLACES)
    work_hours = Decimal(work_hours).quantize(TWOPLACES)
    
    
    if total_logged_hrs >  work_hours : 
        
        #logging.info("Accumulated total logged hours %s is greater than work_hours %s" % (total_logged_hrs, work_hours ))       
        extra_hours = total_logged_hrs - work_hours
        total_time_charge = total_logged_hrs - extra_hours
        
    else:
        total_time_charge =  total_logged_hrs    
    
    total_time_charge = Decimal(total_time_charge).quantize(TWOPLACES)
    extra_hours = Decimal(extra_hours).quantize(TWOPLACES)
    
    #logging.info("total_time_charge : %s" % total_time_charge)
    #logging.info("extra_hours : %s" % extra_hours)
    
    return dict(total_time_charge = total_time_charge, extra_hours = extra_hours, total_logged_hrs = total_logged_hrs )
            
    
            
def readjust_running_balance(doc_time_record_id, now):
    logging.info("Readjust %s" % doc_time_record_id)
    
    database = MySQLdb.connect(**settings.DB_ARGS)
    conn = database.cursor()
    
    
       
    s = couchdb.Server(settings.COUCH_DSN)
    db_time_records = s['rssc_time_records']
    db_client_docs = s['client_docs']
    
    #get timerecord doc
    doc_time_record = db_time_records.get(doc_time_record_id)
    logging.info(doc_time_record)
    
    subcontractors_id = doc_time_record['subcontractors_id']
    leads_id = doc_time_record['leads_id']
    
    #check if prepaid
    db = s['rssc']
    doc_subcon = db.get('subcon-%s' % subcontractors_id)
    
    
    
    a = doc_time_record['time_in']
    time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
    
    
    #Get staff work_status
    sql = "SELECT work_status, client_timezone, staff_working_timezone FROM subcontractors WHERE id=%s;" %  subcontractors_id 
    subcon = conn.execute(sql)    
    subcon = dictfetchall(conn)
    work_status = subcon[0]['work_status']
    client_timezone = subcon[0]['client_timezone']
    staff_working_timezone = subcon[0]['staff_working_timezone']
    conn.close()
    
    
    #Get all recorded logged hours
    result = get_total_logged_hrs(doc_time_record, client_timezone)    
    staff_perspective_reference_date = result['staff_perspective_reference_date']
    client_perspective_reference_date = result['client_perspective_reference_date']    
    total_logged_hrs = result['total_logged_hrs']
    
    #Calculate total hours to be charged 
    result_charge = get_total_charged_hrs(work_status, total_logged_hrs);
    total_time_decimal = result_charge['total_time_charge']
    extra_hours = result_charge['extra_hours']
    total_logged_hrs = result_charge['total_logged_hrs']
    
    #Total hours to be charge    
    logging.info('total_time_decimal :  %s' % total_time_decimal)      
    logging.info('extra_hours :  %s' % extra_hours)
    logging.info('total_logged_hrs :  %s' % total_logged_hrs)
    
    
    if extra_hours > 0:
    
        #check if client has couchdb settings        
        r = db_client_docs.view('client/settings', startkey=[leads_id, now],
            endkey=[leads_id, [2011,1,1,0,0,0,0]], 
            descending=True, limit=1)
      
        if len(r.rows) == 0:    #no client settings, alert devs
            raise Exception ('Failed to credit on finish work. Missing client settings!', 'Please check client settings for leads_id %s. Failed to credit doc rssc_time_records %s' % (leads_id, doc_time_record_id))      
        currency, apply_gst = r.rows[0]['value']
    
        r = db_client_docs.view('client/running_balance', key=leads_id)
    
        if len(r.rows) == 0:
            running_balance = Decimal('0.00').quantize(TWOPLACES)
        else:                                                               
            running_balance = Decimal('%s' % r.rows[0].value).quantize(TWOPLACES)
    
        #get hourly rate from timerecord
        if doc_time_record.has_key('client_hourly_rate') == False:
            send_task('notify_devs.send', ['client_hourly_rate not recorded', 'Please check : %r' % doc_time_record.copy()])
            t = send_task('subcontractors.get_hourly_rate', [subcontractors_id, time_in])
            client_hourly_rate = t.get()
        else:
            client_hourly_rate = Decimal(doc_time_record['client_hourly_rate'])
    
        amount = client_hourly_rate * extra_hours
        credit = Decimal(amount).quantize(TWOPLACES)
    
        particular = 'Reversal for Staff %s %s, <%s> extra worked for %0.2f hours @ %0.2f/hr' % (
            doc_subcon['staff']['fname'], 
            doc_subcon['staff']['lname'], 
            doc_subcon['job_designation'], 
            extra_hours, 
            client_hourly_rate,
            )
        
        
        #apply_gst
        if apply_gst == 'Y':
            credit += credit * Decimal('0.1').quantize(TWOPLACES)
            particular += ' plus GST'
    
        running_balance += credit
    
        doc_transaction = dict(
            added_by = 'RSSC Prepaid On Finish Work',
            added_on = now,
            charge = '0.00',
            client_id = leads_id,                                          
            credit = '%0.2f' % credit,
            credit_type = 'WORK',
            currency = currency,
            remarks = 'Generated from time sheet',
            type = 'credit accounting',
            running_balance = '%0.2f' % running_balance,
            particular = particular,
            doc_subcon = subcontractors_id,
            doc_time_record_id = doc_time_record_id,
            client_hourly_rate = '%0.2f' % client_hourly_rate,
        )
        #logging.info(doc_transaction)
        db_client_docs.save(doc_transaction)
        
        
        #Store the pre calculated adjusted hours and extra hours
        if settings.DEBUG:
            mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
        else:
            mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017) 
                         
        db = mongo_client.timesheet
        col = db.timesheet_details_pre_calculated_adjustments
        mongo_doc = col.find_one({
            'subcontractors_id' : subcontractors_id, 
            'staff_perspective_reference_date' : staff_perspective_reference_date,
            'client_perspective_reference_date' : client_perspective_reference_date
        })
        
        if mongo_doc!=None:        
            col.update(
                {"_id" : ObjectId(mongo_doc["_id"])}, 
                {"$set":{
                    'timestamp' : '%s' % get_ph_time(),
                    'work_status' : work_status,
                    'adj_hrs' : '%s' % total_time_decimal,
                    'extra_hours' : '%s' % extra_hours,
                    'total_logged_hrs' : '%s' % total_logged_hrs
                    }
                }
            )
        else:
            record={
                'userid' : doc_time_record['userid'],    
                'subcontractors_id' : subcontractors_id,    
                'leads_id' : doc_time_record['leads_id'],             
                'staff_perspective_reference_date' : staff_perspective_reference_date,
                'client_perspective_reference_date' : client_perspective_reference_date,
                'reference_date' : staff_perspective_reference_date.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp' : get_ph_time(),
                'work_status' : work_status,
                'adj_hrs' : '%s' % total_time_decimal,
                'extra_hours' : '%s' % extra_hours,
                'total_logged_hrs' : '%s' % total_logged_hrs
            }
            col.insert(record)
        
        
    else:
        logging.info("No extra hours detected")
        
        
        
        
        
    return dict(
        adj_hrs = total_time_decimal,  
        extra_hours = extra_hours, 
        total_logged_hrs = total_logged_hrs,
        reference_date = staff_perspective_reference_date
    )
    
def add_worked_timerecord_with_capping(doc_time_record_id, now, days_before_suspension):
    logging.info("add_worked_timerecord_with_capping")
    
    """generates a charge couchdb document and returns a copy of document
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_time_records = s['rssc_time_records']

    #get timerecord doc
    doc_time_record = db_time_records.get(doc_time_record_id)
    #logging.info(doc_time_record)
    if doc_time_record == None:
        raise Exception ('ERROR PREPAID TIMERECORD',
            'Please check time_record doc_id %s' % doc_time_record_id)

    #check if timerecord is already charged
    if doc_time_record.has_key('doc_charge'):
        raise Exception('%s is already charged to %s' % (doc_time_record_id, doc_time_record['doc_charge']))

    #check if prepaid
    db = s['rssc']
    doc_subcon = db.get('subcon-%s' % doc_time_record['subcontractors_id'])
    #logging.info(doc_subcon)
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
    
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017) 
                    
    db = mongo_client.prod
    col = db.rssc_staff_daily_allowance
    
#     mongo_doc = col.find_one({
#         'couch_id' : doc_time_record_id         
#     })

    mongo_doc = find_one(col, {'couch_id' : doc_time_record_id})
    
    if mongo_doc == None:        
        raise Exception ("Missing rssc_staff_daily_allowance for couch_id : %s" % doc_time_record_id)
    else:
        mongo_doc['work_hours']
        
    logging.info("Amount to be lodge in RBS : %s " % mongo_doc['work_hours'])   
    total_time_decimal = mongo_doc['work_hours']
    total_time_decimal = Decimal('%s' % total_time_decimal).quantize(TWOPLACES)
    
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
    logging.info(doc_transaction)
    db_client_docs.save(doc_transaction)
    
    
    #deduct currency adjustment    
    if days_before_suspension not in [ Decimal('-30.0') ]:
        deduct_currency_adjustment_with_qty(doc_time_record_id,  doc_time_record['subcontractors_id'], total_time_decimal) 
    logging.info("SUCCESS : Generated charge couchdb document")
    
    
def deduct_currency_adjustment_with_qty(doc_id, subcon_id, qty):  
    
    logging.info("deducting currency adjustment %s timerecord %s subcon_id with qty %s " % (doc_id, subcon_id, qty))   
    from StringIO import StringIO
    import pycurl
    
    url = settings.API_URL+"/currency-adjustment/deduct-mongo-to-running-balance/?doc_id="+str(doc_id)+"&subcon_id="+str(subcon_id)+"&qty="+str(qty)
    
    storage = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.WRITEFUNCTION, storage.write)
    c.setopt(pycurl.SSL_VERIFYPEER, 1)
    c.setopt(pycurl.SSL_VERIFYHOST, 2)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.perform()
    c.close()
    
    
def adjust_timesheet(doc_time_record):
    logging.info("Update timesheet_details.adj_hrs")
    
    database = MySQLdb.connect(**settings.DB_ARGS)
    conn = database.cursor()
       
    subcontractors_id = doc_time_record['subcontractors_id']
    logging.info("subcontractors_id : %s" % subcontractors_id)
    phtz = timezone('Asia/Manila')
    
    a = doc_time_record['time_in']    
    month_year_str = datetime(a[0], a[1], 1, 0, 0, 0)
    time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
    logging.info("month_year_str : %s" % month_year_str)
    
    sql = "SELECT id, month_year, status FROM timesheet WHERE status != 'deleted' AND subcontractors_id=%s AND month_year='%s';" %  (subcontractors_id, month_year_str)
    logging.info("sql : %s" % sql)
    timesheet = conn.execute(sql)
    if not timesheet : 
        conn.close()
        logging.info("No timesheet detected for subcontractors_id : %s" % subcontractors_id)  
         
    if timesheet: 
        timesheet = dictfetchall(conn)
        timesheet_id = timesheet[0]['id']
        month_year = timesheet[0]['month_year']
        status = timesheet[0]['status'] 
        
        sql = "SELECT id, hrs_to_be_subcon FROM timesheet_details WHERE timesheet_id=%s AND reference_date=DATE('%s');" %  (timesheet_id, time_in)        
        timesheet_details = conn.execute(sql)
        if timesheet_details: 
            timesheet_details = dictfetchall(conn)
            timesheet_details_id = timesheet_details[0]['id']
            hrs_to_be_subcon =  timesheet_details[0]['hrs_to_be_subcon']
            #diff_paid_vs_adj_hrs =  timesheet[0]['diff_paid_vs_adj_hrs']            
                            
            conn.close()        
            logging.info("timesheet_id : %s" % timesheet_id)
        
            check_if_logged_in = check_if_already_logged_in(doc_time_record)            
            #print check_if_logged_in["sum"]
            if not check_if_logged_in["sum"]:
                logging.info("check_if_logged.sum is NULL")
                check_if_logged_in["sum"] = 0

            if  hrs_to_be_subcon == None:
                hrs_to_be_subcon = 0   
                 
                    
            #Update timesheet_details.adj_hrs 
            #new cursor for updating and insertion. As per Allan advice
            db_update = MySQLdb.connect(**settings.DB_ARGS)
            cursor = db_update.cursor()

            diff_paid_vs_adj_hrs = check_if_logged_in["sum"] - hrs_to_be_subcon
    
            #sql = "UPDATE timesheet_details SET status='locked', adj_hrs=%s, diff_paid_vs_adj_hrs=%s WHERE timesheet_id=%s AND reference_date=DATE('%s');" % (check_if_logged_in["sum"], diff_paid_vs_adj_hrs, timesheet_id, time_in)
            sql = "UPDATE timesheet_details SET status='locked', adj_hrs=%s, diff_paid_vs_adj_hrs=%s WHERE id=%s;" % (check_if_logged_in["sum"], diff_paid_vs_adj_hrs, timesheet_details_id)
            logging.info(sql)
            cursor.execute(sql)            
            cursor.execute("set autocommit = 1")
            
            #close mysql connections
            db_update.commit()    
            cursor.close()            
   