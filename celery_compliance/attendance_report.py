#   2014-05-02  Normaneil Macutay<normanm@remotestaff.com.au>
#   - Initial Hack.  Task to get subcon login date/time in couchdb rssc_time_records

import settings
import couchdb
import re
import string
from pprint import pprint, pformat

from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet


from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal, ROUND_HALF_UP

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
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

@task(ignore_result=True)    
def process_doc_id(doc_id):
    logging.info('attendance_report.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
       
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
    
        
    subcontractor_ids = doc['subcontractor_ids']
    DATE_SEARCH = doc['date_search']    

    
    #return reference_doc
    
    str=""
    couch_result={}
    total_reg_hours={}
    total_login_types={}
    subcon_staff_timezone = 'Asia/Manila'
    
    for sid in subcontractor_ids:
        compliance_result =  doc['compliance_result'][sid] 
        registered_hrs_result = doc['registered_hrs_result'][sid] 
        subcon_staff_timezone = doc['subcon_staff_timezone'][sid]
        
        subcon_adj_hrs_result = doc['subcon_adj_hrs_result'][sid]
        subcon_log_hrs_result = doc['subcon_log_hrs_result'][sid]     
        
        
        dates=[]    
        staff_start_work_hour=""
        staff_finish_work_hour=""        
        timein=""        
        regular_contract_hrs=0
        total_regular_contract_hrs = 0
                
        total_present = 0
        total_late = 0
        total_early_login = 0
        total_extra_day = 0
        total_flexi = 0
        total_absent =0
        total_on_leave =0
        total_no_schedule =0
        total_not_yet_working =0        
        
        for d in DATE_SEARCH: 
            date_obj = datetime.strptime(d, '%Y-%m-%d')
            present = 0
            late = 0
            early_login = 0
            extra_day = 0
            flexi = 0
            absent =0
            on_leave =0
            no_schedule =0
            not_yet_working =0

            log_hrs=0.0
            adj_hrs=0.0            
            
            #staff registered rostered hours    
            for reg in registered_hrs_result:
                if d == reg['date']:
                    staff_start_work_hour = reg['staff_start_work_hour']
                    staff_finish_work_hour = reg['staff_finish_work_hour']                    
                    regular_contract_hrs = reg['regular_contract_hrs']                    
            total_regular_contract_hrs = total_regular_contract_hrs + regular_contract_hrs
            
            #subcon adjusted hours
            for s in subcon_adj_hrs_result:
                if d == s['date']:
                    adj_hrs = s['adj_hrs']

            #subcon log hours
            for s in subcon_log_hrs_result:
                if d == s['date']:
                    log_hrs = s['log_hrs']            
            
            for c in compliance_result:
                if d == c['date']:
                    timein = c['timein'] 
                    compliance = c['compliance']

            if compliance == 'present':
                present = 1
                total_present = total_present + 1                
            if compliance == 'late':
                late = 1
                total_late = total_late + 1                
            if compliance == 'early login':
                early_login = 1
                total_early_login = total_early_login + 1                
            if compliance == 'extra day':
                extra_day = 1
                total_extra_day = total_extra_day + 1                
            if compliance == 'flexi':
                flexi = 1
                total_flexi = total_flexi + 1                
            if compliance == 'absent':
                absent = 1
                total_absent = total_absent + 1                
            if compliance == 'on leave':
                on_leave = 1
                total_on_leave = total_on_leave + 1                 
            if compliance == 'no schedule':
                no_schedule = 1
                total_no_schedule = total_no_schedule + 1                
            if compliance == 'not yet working':
                not_yet_working = 1 
                total_not_yet_working = total_not_yet_working + 1            

            if staff_start_work_hour:
                staff_start_work_hour = '%s %s' % (d, staff_start_work_hour)
                staff_start_work_hour = datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')
                staff_start_work_hour = staff_start_work_hour.strftime('%I:%M %p')

            if staff_finish_work_hour:
                staff_finish_work_hour = '%s %s' % (d, staff_finish_work_hour)
                staff_finish_work_hour = datetime.strptime(staff_finish_work_hour, '%Y-%m-%d %H:%M:%S')
                staff_finish_work_hour = staff_finish_work_hour.strftime('%I:%M %p')
                
            dates.append(dict(
                date = '%s' % d,
                compliance = '%s' % compliance,
                timein = timein,                
                staff_start_work_hour = '%s' % staff_start_work_hour,
                staff_finish_work_hour = '%s' % staff_finish_work_hour,                
                regular_contract_hrs = '%d' % regular_contract_hrs,
                worked_hrs = '%s' % log_hrs,
                adj_hrs = '%s' % adj_hrs,
                present = '%d' % present,
                late = '%d' % late, 
                early_login = '%d' % early_login,
                extra_day = '%d' % extra_day,
                flexi = '%d' % flexi,
                absent = '%d' % absent,
                on_leave = '%d' % on_leave,
                no_schedule = '%d' % no_schedule, 
                not_yet_working = '%d' % not_yet_working,
                subcon_staff_timezone = '%s' % subcon_staff_timezone                
                )
            )
        login_types = dict(
            total_present = '%d' % total_present,
            total_late = '%d' % total_late, 
            total_early_login = '%d' % total_early_login,
            total_extra_day = '%d' % total_extra_day,
            total_flexi = '%d' % total_flexi,
            total_absent = '%d' % total_absent,
            total_on_leave = '%d' % total_on_leave,
            total_no_schedule = '%d' % total_no_schedule, 
            total_not_yet_working = '%d' % total_not_yet_working            
        )            
            
            
        couch_result[int(sid)] = dates
        total_reg_hours[int(sid)] = total_regular_contract_hrs
        total_login_types[int(sid)] = login_types        

        
    doc['result'] = couch_result
    doc['total_reg_hours'] = total_reg_hours
    doc['total_login_types'] = total_login_types   
    doc['result_date_time'] = get_ph_time().strftime('%Y-%m-%d %H:%M:%S')    
    db.save(doc)        
            
    #return dates            
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process_doc_id('bc7205ad9e81255ebe9b4b7496003c7b'))
