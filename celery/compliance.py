#   2014-05-02  Normaneil Macutay<normanm@remotestaff.com.au>
#   - Initial Hack.  Task to get subcon login date/time in couchdb rssc_time_records

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

from celery import Celery
import sc_celeryconfig


from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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
    time = time + timedelta(minutes=5)
    return time    

def get_ten_minutes_before_time(time):
    time = time - timedelta(minutes=10)
    return time
    
def    get_two_hours_after_time(time):
    time = time + timedelta(hours=2)
    return time

@task    
def get_subcon_compliance_result(sid, date_str, userid, leads_id, starting_date, work_days, subcon_working_days, subcon_client_working_hours, num_of_leave, num_of_marked_absent, complete=False ):
    """
    Used in List of Subcontractors
    """
    
    
    
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
    if time_in:            
        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))            
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
            
    else:
        if int(num_of_marked_absent) > 0:
            compliance = 'absent'        
        if int(num_of_marked_absent) == 0:    
            if day in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'        
                else:
                    two_hours_after_time = get_two_hours_after_time(staff_start_work_hour)
                    if flexi == 'no':
                        if complete: # used by Running Late page
                            if current_time < two_hours_after_time:                    
                                if staff_start_work_hour < current_time:
                                    compliance = 'running late'
                                else:
                                    if current_time >= get_ten_minutes_before_time(staff_start_work_hour) and current_time <= staff_start_work_hour:
                                        compliance = '10 minutes'                            
                                    else:
                                        compliance = "not yet working"
                            else:
                                if int(num_of_leave) > 0:
                                    compliance='on leave'

                                else:                    
                                    compliance = 'absent'
                        
                        else:                    
                            if current_time < two_hours_after_time:
                                compliance = "not yet working"
                            else:
                                compliance = 'absent'
                    else:
                        compliance = 'flexi'                
                
            if day not in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'
                else:                    
                    compliance = 'no schedule'            
                    
    #if starting_date > d:
    #    compliance = "not yet working"
        
    return {'timein' : '%s' % login, 'compliance' : compliance}     

@task        
def get_compliance_result(conn, sid, date_str, userid, leads_id, starting_date, work_days, subcon_working_days, subcon_client_working_hours, complete=False):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
    
    num_of_leave=0
    num_of_marked_absent=0
    
    sql = text("""SELECT COUNT(l.id)AS num_of_leave FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE l.date_of_leave=:date_str AND l.status='approved' AND r.userid=:userid and r.leads_id=:leads_id""")
    leave = conn.execute(sql, date_str='%s' % date_str, userid=userid, leads_id=leads_id).fetchone()
    if leave:
        num_of_leave = leave.num_of_leave    
    
    sql = text("""SELECT COUNT(l.id)AS num_of_marked_absent FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE l.date_of_leave=:date_str AND l.status='absent' AND r.userid=:userid and r.leads_id=:leads_id""")
    leave = conn.execute(sql, date_str='%s' % date_str, userid=userid, leads_id=leads_id).fetchone()    
    if leave:
        num_of_marked_absent = leave.num_of_marked_absent     
        
    
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

    """         
    if day not in work_days:
        staff_start_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon_client_working_hours['client_start_work_hour']))
        staff_start_work_hour = client_tz.localize(datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)    
        staff_start_work_hour = staff_start_work_hour.strftime('%H:%M:%S') 
    
    if not staff_start_work_hour:
        staff_start_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon_client_working_hours['client_start_work_hour']))
        staff_start_work_hour = client_tz.localize(datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)    
        staff_start_work_hour = staff_start_work_hour.strftime('%H:%M:%S')
    """            
    
    staff_start_work_hour =  '%s %s' % (date_str, staff_start_work_hour)
    staff_start_work_hour = datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')        
    
    five_minutes_before_time=""    
    five_minutes_after_time=""
    two_hours_after_time =""
    ten_minutes_before_time=""       
    current_time = get_ph_time()
    compliance =""
    login=""    
    if time_in:            
        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))            
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
            
    else:
        if int(num_of_marked_absent) > 0:
            compliance = 'absent'        
        if int(num_of_marked_absent) == 0:    
            if day in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'        
                else:
                    two_hours_after_time = get_two_hours_after_time(staff_start_work_hour)
                    if flexi == 'no':
                        if complete: # used by Running Late page
                            if current_time < two_hours_after_time:                    
                                if staff_start_work_hour < current_time:
                                    compliance = 'running late'
                                else:
                                    if current_time >= get_ten_minutes_before_time(staff_start_work_hour) and current_time <= staff_start_work_hour:
                                        compliance = '10 minutes'                            
                                    else:
                                        compliance = "not yet working"
                            else:
                                if int(num_of_leave) > 0:
                                    compliance='on leave'

                                else:                    
                                    compliance = 'absent'
                        
                        else:                    
                            if current_time < two_hours_after_time:
                                compliance = "not yet working"
                            else:
                                compliance = 'absent'
                    else:
                        compliance = 'flexi'                
                
            if day not in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'
                else:                    
                    compliance = 'no schedule'            
                    
    #if starting_date > d:
    #    compliance = "not yet working"
        
    return {'timein' : '%s' % login, 'compliance' : compliance}
        
@task(ignore_result=True)
def process_doc_id(doc_id):
    logging.info('compliance.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    
    #send notice to devs
    now = get_ph_time(as_array=False) 
    to=['devs@remotestaff.com.au']
    couch_mailbox = s['mailbox']    
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]          
    mailbox = dict(
        sent = False,        
        bcc = None,
        cc = None,
        created = date_created,
        generated_by = 'celery compliance.process_doc_id',
        html = None,
        text = 'executing celery task compliance.process_doc_id %s' % doc_id,        
        subject = 'executing celery task compliance.process_doc_id %s' % doc_id,
        to = to            
    )
    mailbox['from'] = 'noreply@remotestaff.com.au'        
    #couch_mailbox.save(mailbox)    
    
    #subconlist_reporting doc    
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
        
    subcontractor_ids = doc['subcontractor_ids']
    DATE_SEARCH = doc['date_search']
    page_usage=""
    complete = False    
    if 'page_usage' in doc:    
        page_usage = doc['page_usage']
        
    if page_usage == "running late":
        complete =True


        
    str =""    
    compliance_result={}
    registered_hrs_result={}
    conn = engine.connect()
    
        
      
    for sid in subcontractor_ids:
        userid =  doc['subcon_userid'][sid]
        leads_id =  doc['subcon_leads_id'][sid]
        starting_date =  doc['subcon_starting_date'][sid]
        work_days =  doc['subcon_work_days'][sid].split(',')
        subcon_working_days =  doc['subcon_working_days'][sid]         
        subcon_client_working_hours = doc['subcon_client_working_hours'][sid]        
        
        dates=[]
        registered_hrs=[]                  
        for d in DATE_SEARCH:
            if sid:        
                compliance = get_compliance_result(conn, sid, d, userid, leads_id, starting_date, work_days, subcon_working_days, subcon_client_working_hours, True)
                #return compliance                
                dates.append(dict(
                    date = d,
                    compliance = compliance['compliance'],
                    timein = compliance['timein']                
                    )
                )   
            
            date = datetime.strptime(d, '%Y-%m-%d')            
            day = date.strftime('%a')            
            day = day.lower()
            staff_start_work_hour=""
            staff_finish_work_hour=""            
            regular_contract_hrs=0            
            for work in subcon_working_days:
                if work['day'] == day:
                    staff_start_work_hour = work['staff_start_work_hour']
                    staff_finish_work_hour = work['staff_finish_work_hour']                    
                    regular_contract_hrs = work['regular_contract_hrs']                    
                    break
                    
            registered_hrs.append(dict(
                date = d,
                staff_start_work_hour = staff_start_work_hour,
                staff_finish_work_hour = staff_finish_work_hour,                
                regular_contract_hrs = regular_contract_hrs                
                )
            )                        
            #str +='\n sid=>%s %s' % (sid, compliance)                 
                        
        compliance_result[int(sid)] = dates
        registered_hrs_result[int(sid)] = registered_hrs        
        
    doc['compliance_result'] = compliance_result
    doc['registered_hrs_result'] = registered_hrs_result
    
    conn.close()    
    db.save(doc)    
    
    if page_usage == "attendance report":
        if settings.DEBUG:
            send_task("subcon_adj_hours.process_doc_id", [doc_id])
        else:
            celery = Celery()
            celery.config_from_object(sc_celeryconfig)
            celery.send_task("subcon_adj_hours.process_doc_id", [doc_id])        
        logging.info('executing subcon_adj_hours.process_doc_id %s' % doc_id)
        
        #send email        
        to=['devs@remotestaff.com.au']
        couch_mailbox = s['mailbox']    
        date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]          
        mailbox = dict(
            sent = False,        
            bcc = None,
            cc = None,
            created = date_created,
            generated_by = 'celery compliance.process_doc_id',
            html = None,
            text = 'sending task to subcon_adj_hours.process_doc_id %s' % doc_id,        
            subject = 'sending task to subcon_adj_hours.process_doc_id %s' % doc_id,
            to = to            
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'        
        #couch_mailbox.save(mailbox)
            
    
    
if __name__ == '__main__':
    logging.info('tests')
    #logging.info(process_doc_id('bc7205ad9e81255ebe9b4b7496003c7b'))
    logging.info(get_subcon_compliance_result(3046,'2013-08-19',74,11,'2008-08-01','mon,tue,wed,thu,fri',[{'regular_contract_hrs': 4, 'staff_finish_work_hour': '11:00:00', 'day': 'mon', 'staff_start_work_hour': '07:00:00'}],{'flexi': u'no', 'client_finish_work_hour': '13:00:00', 'staff_working_timezone': u'Asia/Manila', 'client_timezone': u'Australia/Melbourne', 'client_start_work_hour': '09:00:00'},False,0,0))
