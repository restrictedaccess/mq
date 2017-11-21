#!/usr/bin/env python
#   2015-01-19  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   initial commit
#   -   script to get all active subcons compliance result
#   -   updating mongodb document
#   2015-01-19  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   removed persistent_mysql_connection 
#   -   used MySQLdb as per Allanaire Tapion

from celery.task import task, Task
from celery.execute import send_task
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal, ROUND_HALF_UP
import MySQLdb
import couchdb
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
import settings
from pymongo import MongoClient
from bson.objectid import ObjectId

TWOPLACES = Decimal(10) ** -2

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
    
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
        
def get_five_minutes_before_time(time):
    time = time - timedelta(minutes=5)
    return time	
	
def get_five_minutes_after_time(time):
    time = time + timedelta(minutes=6)
    return time	

def get_ten_minutes_before_time(time):
    time = time - timedelta(minutes=10)
    return time
	
def	get_two_hours_after_time(time):
    time = time + timedelta(hours=2)
    return time

def get_total_log_hrs(sid, start_date, end_date, userid):
    now = get_ph_time()    
    total_work_hours = Decimal('0.00')
    
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']    
    
    userid = int(int(userid))                
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

                        

    return "%0.2f" % total_work_hours

@task		
def get_compliance_result(conn, sid, date_str, userid, leads_id, starting_date, work_days, subcon_working_days, subcon_client_working_hours, complete=False):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
	
     
    
    num_of_leave=0
    num_of_marked_absent=0
	
    sql = "SELECT COUNT(l.id)AS num_of_leave FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE DATE(l.date_of_leave)='%s' AND l.status='approved' AND r.userid=%s and r.leads_id=%s" % ('%s' % date_str, userid, leads_id)
    leave = conn.execute(sql)
    if leave:
        leave = dictfetchall(conn)
        num_of_leave = leave[0]['num_of_leave']	
	
    sql = "SELECT COUNT(l.id)AS num_of_marked_absent FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE DATE(l.date_of_leave)='%s' AND l.status='absent' AND r.userid=%s and r.leads_id=%s" % ('%s' % date_str, userid, leads_id)
    leave = conn.execute(sql)
    if leave:
        leave = dictfetchall(conn)
        num_of_marked_absent = leave[0]['num_of_marked_absent']
    	
	
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']
	
    d = datetime.strptime(date_str, '%Y-%m-%d')
    starting_date = datetime.strptime(starting_date, '%Y-%m-%d') 	
    from_year = '%s' % d.strftime('%Y')
    from_month = '%s' % d.strftime('%m')
    from_day = '%s' % d.strftime('%d')
    time_in=""
    timein=d	

    #get the first time in of staff	
    userid = int(userid)				
    r = db.view('rssc_reports/userid_timein', 
        startkey=[userid, [ int(from_year), int(from_month), int(from_day),0,0,0,0]], 
        endkey=[userid, [ int(from_year), int(from_month), int(from_day),23,59,59,0]],
        descending=False			
    )
    for row in r.rows:            		
        mode, time_out, leads_id, subcon_id = row['value']
        if subcon_id != None:		
            if int(subcon_id) == int(sid):		
                if mode == 'time record' :
                    userid, time_in = row['key']
                    break
    time_out = ""
    for row in r.rows:            		
        mode, time_out, leads_id, subcon_id = row['value'] 
        if subcon_id != None:		
            if int(subcon_id) == int(sid):		
                if mode == 'time record' :
                    mode, time_out, leads_id, subcon_id = row['value']
                    break
            else:
                time_out=""    
	
    day = d.strftime('%a')			
    day = day.lower()	
    	
    #get the staff working hours
    staff_start_work_hour=""
    staff_finish_work_hour=""
    regular_contract_hrs=0
    
    client_tz = timezone(subcon_client_working_hours['client_timezone'])
    ph_tz = timezone(subcon_client_working_hours['staff_working_timezone']) 	
    flexi = subcon_client_working_hours['flexi']	
    str = ""	
    for work in subcon_working_days:
        if work['day'] == day:
            staff_start_work_hour = work['staff_start_work_hour']   			
            break

   		
	
    staff_start_work_hour =  '%s %s' % (date_str, staff_start_work_hour)
    staff_start_work_hour = datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')		
	
    five_minutes_before_time=""	
    five_minutes_after_time=""
    two_hours_after_time =""
    ten_minutes_before_time=""       
    current_time = get_ph_time()
    compliance =""
    login=""
    logout=""
    working_status=""
    
    if time_out:			
        logout = datetime(int(time_out[0]), int(time_out[1]), int(time_out[2]), int(time_out[3]), int(time_out[4]), int(time_out[5]))
        #end = datetime(int(time_out[0]), int(time_out[1]), int(time_out[2]), int(time_out[3]), int(time_out[4]), int(time_out[5]), tzinfo=ph_tz)
        
    if time_in:			
        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))
        #start = datetime(aint(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]), tzinfo=ph_tz)    
        if d.strftime('%Y-%m-%d') == login.strftime('%Y-%m-%d'):
            timein = login
				
        five_minutes_before_time = get_five_minutes_before_time(staff_start_work_hour)
        five_minutes_after_time = get_five_minutes_after_time(staff_start_work_hour)			
        if day not in work_days:
            if flexi == 'no':		
                compliance = 'extra day'            
            else:
                compliance = 'flexi'

        if day in work_days:
            if flexi == 'no':  		
                if timein < five_minutes_before_time:
                    compliance = 'early login'
					
                if timein > five_minutes_after_time:
                    compliance = 'late'

                if timein >= five_minutes_before_time and timein <= five_minutes_after_time:
                    compliance = 'present'
            else:
                compliance = 'flexi'

        working_status = 'working'            
        	
    else:
        working_status = 'not working'
        
        if int(num_of_marked_absent) > 0:
            compliance = 'marked absent'		
        if int(num_of_marked_absent) == 0:	
            if day in work_days:

                if int(num_of_leave) > 0:
                    compliance='approved leave'		
                else:                   
                    if flexi == 'no':
                        compliance = 'absent'						                       
                    else:
                        compliance = 'flexi'				
				
            if day not in work_days:
                if int(num_of_leave) > 0:
                    compliance='approved leave'
                else:					
                    compliance = 'no schedule'
                    
    total_work_hours = Decimal('0.00')               
    total_work_hours = get_total_log_hrs(sid, date_str, date_str, userid)	
    return {'timein' : '%s' % login, 'timeout' : '%s' % logout, 'total_work_hours' : total_work_hours, 'compliance' : compliance, 'working_status' : working_status}
    
@task(ignore_result=True)
def process(id):
    logging.info('executing get_staff_daily_attendance_result.process(%s)' % id)
    
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    
    
    try:
        conn = db.cursor()
        sql = "SELECT mongodb_doc_id FROM mongo_staff_daily_attendance m  WHERE id=%s;"  % id
        conn.execute(sql)
        row = dictfetchall(conn)
        doc_id = row[0]['mongodb_doc_id']    
        conn.close()
    except:
        return "error in retrieving mongo_staff_daily_attendance.id=%s " % id
        
    
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST)
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD)
            
    #connect to the test db
    mongodb = mongo_client.reports
    #retrieve the staff_daily_attendance collection
    col = mongodb.staff_daily_attendance
    
    
    try:
        doc = col.find_one({"_id" : ObjectId(doc_id)})
    except:
        return 'invalid mongodb document id'
            
    if not doc:
        return 'mongodb document id not found'
    
    #return doc
    subcontractor_ids = doc['subcontractor_ids']
    DATE_SEARCH = doc['date_search'][0]
    complete = False	


    #return DATE_SEARCH
        
    str="\n\nResults:\n"	
    compliance_result={}
    registered_hrs_result={}
    conn = db.cursor()
    
    total_absent=0
    total_approve_leave=0
    total_working =0
    total_not_working=0    
    total_marked_absent=0
    
    result = []
    
    
    for c in doc['currencies']:
              
        str +='=> %s\n' % (c['currency'])
        currency ='%s' % c['currency']
        
        
        absent=0
        approve_leave=0
        working =0
        not_working=0    
        marked_absent=0
        
        subcons=[]
        
        subcons_working=[]
        subcons_not_working=[]
        for sid in c['subcons']:
            
            userid =  doc['subcon_userid'][sid]
            leads_id =  doc['subcon_leads_id'][sid]
            starting_date =  doc['subcon_starting_date'][sid]
            work_days =  doc['subcon_work_days'][sid].split(',')
            subcon_working_days =  doc['subcon_working_days'][sid] 		
            subcon_client_working_hours = doc['subcon_client_working_hours'][sid]

            dates=[]
            registered_hrs=[]
        
            compliance = get_compliance_result(conn, sid, DATE_SEARCH, userid, leads_id, starting_date, work_days, subcon_working_days, subcon_client_working_hours, True)
            
            if compliance['working_status'] == 'working':
                working = working + 1
                subcons_working.append(dict(
                    subcon_id = int(sid),
                    record = compliance,
                    )
                )

            if compliance['working_status'] == 'not working':
                not_working = not_working + 1
                subcons_not_working.append(dict(
                    subcon_id = int(sid),
                    record = compliance,
                    )
                )
                
                if compliance['compliance'] == 'marked absent':
                    marked_absent = marked_absent + 1
                    
                if compliance['compliance'] == 'approved leave':
                    approve_leave = approve_leave + 1

                if compliance['compliance'] == 'absent':
                    absent = absent + 1
                
            
            subcons.append(dict(
                subcon_id = int(sid),
                record = compliance,                
                )
            )
            str +='=> %s %s\n\n' % (sid, dates)
        
        total_working = total_working + working
        total_not_working = total_not_working + not_working
        total_absent= total_absent + absent
        total_approve_leave= total_approve_leave + approve_leave
        total_marked_absent= total_marked_absent + marked_absent
        
        
        result.append(dict(
            currency = currency,
            subcons = subcons,
            working = working,
            not_working = not_working,
            approve_leave = approve_leave,
            marked_absent = marked_absent,
            absent = absent,
            subcons_working = subcons_working,
            subcons_not_working = subcons_not_working        
            )
        )    
            
            
            
            
    #return result
    
    #update the document
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'result' : result}})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'absent' : '%s' % total_absent }})  
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'working' : '%s' % total_working }})  
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'not_working' : '%s' % total_not_working }})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'marked_absent' : '%s' % total_marked_absent }})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'approve_leave' : '%s' % total_approve_leave }})  
           
    
    #close the current connection of mysqldb
    conn.close()
    
    #open up new mysqldb connection for updating record
    db_update = MySQLdb.connect(**settings.DB_ARGS)
    cursor = db_update.cursor()
    sql = "UPDATE mongo_staff_daily_attendance SET status='executed', date_executed='%s' WHERE id='%s';" % ('%s' % get_ph_time(), id)
    cursor.execute(sql)
    db_update.commit()    
    cursor.close()

    logging.info('frinished executing get_staff_daily_attendance_result.process(%s)' % id)
    
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process(1))