#!/usr/bin/env python
#   2015-01-19  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   initial commit
#   -   script to get all active subcons on a given date
#   -   creating mongodb document
#   -   suggested daily schedule execution from 1am to 3am.


from sqlalchemy.sql import text
from persistent_mysql_connection import engine

from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

from decimal import Decimal


import couchdb

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import settings

from pymongo import MongoClient

import get_staff_daily_attendance_result

   
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
        
def getClientCouchCurrencyandGst(leads_id):
    client_id = int(leads_id)
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
    
    #get client couchdb settings
    now = get_ph_time()
    
    now = [now.year, now.month, now.day, now.hour, now.minute, now.second, 0]
    r = db.view('client/settings', startkey=[client_id, now],
        endkey=[client_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1)
    #		
    if len(r) == 0:    #no client settings, raise error
        couch_currency = 'None'
        couch_apply_gst = 'N' 			
    else:		
        couch_currency, couch_apply_gst = r.rows[0]['value']
        
    return couch_currency        

@task(ignore_result=True)
def process(date_str=None):
    logging.info('executing get_staff_daily_attendance.process %s' % get_ph_time())
    if date_str:
        now = datetime.strptime(date_str, '%Y-%m-%d')
    else:
        now = get_ph_time()
        now = now - timedelta(days=1)
    
    start_date = now
    end_date = now
    
    #return now    
    weekday_name = now.strftime('%a')			
    weekday_name = weekday_name.lower() 
    weekday_start = '%s_start' % weekday_name
    weekday_finish = '%s_finish' % weekday_name
    weekday_number_hrs = '%s_number_hrs' % weekday_name	
    #return weekday_name
    
    
    conn = engine.connect()
    query = "SELECT s.id, s.userid, s.starting_date, s.end_date, s.status FROM subcontractors s JOIN leads l ON l.id = s.leads_id WHERE s.status in ('ACTIVE', 'suspended', 'terminated', 'resigned')"
    subcons = conn.execute(query).fetchall()
    #str="\n\nResults:\n"
    
    subcons_str=""
    for	s in subcons:
        sid = '%d' % s.id
        starting_date = '%s' % s.starting_date
        status = s.status
        ending_date = '%s' % s.end_date
        sid = int(sid)
        
        starting_date = datetime.strptime(starting_date, '%Y-%m-%d')
        if status == 'ACTIVE' or status == 'suspended' :
            if starting_date <= end_date:
                subcons_str += '%d,' % sid        
                #str += 'subcon_id : %s starting_date : %s status : %s end_date : %s\n\n' % (sid, starting_date, status, ending_date)
    
        if status == 'terminated' or status == 'resigned' :
            if ending_date != 'None':
                ending_date = datetime.strptime(ending_date, '%Y-%m-%d %H:%M:%S')
                ending_date = datetime.strptime('%s' % ending_date.strftime('%Y-%m-%d'), '%Y-%m-%d')				
			
                if starting_date <= end_date:
                    if ending_date >= start_date and ending_date >= end_date: 			
                        subcons_str += '%d,' % sid
                        #str += 'subcon_id : %s starting_date : %s status : %s end_date : %s\n\n' % (sid, starting_date, status, ending_date)
                    if ending_date >= start_date and ending_date <= end_date:  				                              
                        subcons_str += '%d,' % sid
                        #str += 'subcon_id : %s starting_date : %s status : %s end_date : %s\n\n' % (sid, starting_date, status, ending_date)
    
    
    
    
    
    order_by_str = 'order by p.fname ASC'
    sql = "select s.id, s.userid, s.leads_id, s.starting_date, s.work_days, s.flexi, "
    sql += "staff_working_timezone, client_timezone, client_start_work_hour, client_finish_work_hour, "
    sql += "(%s)AS staff_start_hr, (%s)as staff_finish_hr, (%s)as staff_num_hrs " % (weekday_start, weekday_finish, weekday_number_hrs)	
    sql += "from subcontractors s "
    sql += "left join personal p on p.userid=s.userid "
    sql += "left join leads l on l.id=s.leads_id "
    sql += "where s.id not in (select subcon_id from client_subcontractors_lookup where subcon_id is not null) " 
    sql += "and s.id in(%s) " % subcons_str[:-1]
    sql += "and date(s.starting_date)<='%s' order by p.fname;" % now.strftime('%Y-%m-%d')
    #sql += "and s.status in('ACTIVE', 'suspended', 'terminated', 'resigned')"
    #sql += "and s.status in('ACTIVE', 'suspended')"
    #sql += "and s.id in(1345, 1713, 3142, 3612)"
    #sql += "and s.leads_id not in(11)"
    #sql += "and s.leads_id in(11, 8300, 9587)"
    #sql += "and date(s.starting_date)<='%s' order by p.fname;" % now.strftime('%Y-%m-%d')
    #return sql
    subcons = conn.execute(sql).fetchall()
    
    
    #return subcons
    data=[]	
    subcon_userid = {}
    subcon_leads_id = {}
    subcon_starting_date= {}
    subcon_work_days={}
	
    subcon_working_days={}
    subcon_client_working_hours={}	
    subcon_staff_timezone={}
   
    staff_start_work_hour=None
    staff_finish_work_hour=None	

    have_sched = True
    staff_start_hr = None
    staff_num_hrs = 0
    str="\n\nResults:\n"
    
    subcon_client_currency={}
    currency=""
    aud_currency=0
    usd_currency=0
    gbp_currency=0
    no_currency=0
    
    currencies=[]
    subcon_aud_currencies=[]
    subcon_usd_currencies=[]
    subcon_gbp_currencies=[]
    subcon_none_currencies=[]
    
    for	s in subcons:
        sid = '%d' % s.id
        userid = '%d' % s.userid
        leads_id = '%d' % s.leads_id
        work_days_str = '%s' % s.work_days
        currency = getClientCouchCurrencyandGst(leads_id)
        
        if currency == "AUD":
            aud_currency = aud_currency + 1
            subcon_aud_currencies.append(sid)
            
        if currency == "USD":
            usd_currency = usd_currency + 1 
            subcon_usd_currencies.append(sid)
            
        if currency == "GBP":
            gbp_currency = gbp_currency + 1
            subcon_gbp_currencies.append(sid)
            
        if currency == "None":
            no_currency = no_currency + 1    
            subcon_none_currencies.append(sid)
            
        if not work_days_str:
            work_days_str = 'mon,tue,wed,thu,fri'
        
        
        staff_start_work_hour = s.staff_start_hr
        staff_finish_work_hour = s.staff_finish_hr
        if s.staff_num_hrs:
            staff_num_hrs = s.staff_num_hrs
            
        ph_tz = timezone('%s' % s.staff_working_timezone)
        client_tz = timezone('%s' % s.client_timezone)
        
        data.append(sid)   
        subcon_userid[sid] = userid
        subcon_leads_id[sid] = leads_id
        subcon_work_days[sid] = work_days_str
        subcon_starting_date[sid] = '%s' % s.starting_date
        subcon_client_currency[sid] = '%s' % currency
        
        work_days = work_days_str.split(",")
        working_days=[]
		
        if weekday_name in work_days:
            have_sched = True 		 
			
        if weekday_name not in work_days:
            have_sched = False
        
        client_working_hours={'client_start_work_hour' : '%s' % s.client_start_work_hour, 
            'client_finish_work_hour' : '%s' % s.client_finish_work_hour, 
            'client_timezone' : '%s' % s.client_timezone, 
            'staff_working_timezone' : '%s' %  s.staff_working_timezone, 
            'flexi' : '%s' % s.flexi 
        }
        
        
        if staff_start_work_hour == None or not have_sched:		
            staff_start_work_hour = '%s %s' % (now.strftime('%Y-%m-%d'), (s.client_start_work_hour))
            staff_start_work_hour = client_tz.localize(datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)	
            staff_start_work_hour = '%s' % staff_start_work_hour.strftime('%H:%M:%S')
			
        if staff_finish_work_hour == None or not have_sched:		
            staff_finish_work_hour = '%s %s' % (now.strftime('%Y-%m-%d'), (s.client_finish_work_hour))
            staff_finish_work_hour = client_tz.localize(datetime.strptime(staff_finish_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)	
            staff_finish_work_hour = '%s' % staff_finish_work_hour.strftime('%H:%M:%S')
            
        working_days.append(dict(
            day = '%s' % weekday_name,			
            staff_start_work_hour = '%s' % staff_start_work_hour,
            staff_finish_work_hour = '%s' % staff_finish_work_hour,
            regular_contract_hrs = '%d' %  staff_num_hrs				
            )
        )

        
        subcon_working_days[sid] = working_days		
        subcon_client_working_hours[sid] = client_working_hours
        subcon_staff_timezone[sid] = '%s' % s.staff_working_timezone
            
        #str +='=> %s %s %s \n' % (sid, staff_start_work_hour, staff_finish_work_hour) 
		
   
    doc_id = ""
    query = ""
    
    
    if settings.DEBUG:
        #mongo_client = MongoClient(host=settings.MONGO_TEST)
        #mongo_client = MongoClient()
        try:
            mongo_client = MongoClient()
        except:
            mongo_client = MongoClient(host=settings.MONGO_TEST)
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017)
        
    #mongo_client = MongoClient(host=MONGO_TEST)
    #connect to the test db
    db = mongo_client.reports
    #retrieve the person collection
    col = db.staff_daily_attendance
    #creating a document - CREATE        
    #col.insert({"requested_on": '%s' % now}, {"subcon_userid": {"key1" : 1, "key2": 2}})
    
    currencies=[
        {'currency' : 'AUD', 'total': '%s' % aud_currency, 'subcons' : subcon_aud_currencies},
        {'currency' : 'USD', 'total': '%s' % usd_currency, 'subcons' : subcon_usd_currencies},
        {'currency' : 'GBP', 'total': '%s' % gbp_currency, 'subcons' : subcon_gbp_currencies},
        {'currency' : 'None', 'total': '%s' % no_currency, 'subcons' : subcon_none_currencies},
    ]
    
    record={'requested_on' : '%s' % get_ph_time(), 
        'subcontractor_ids' : data,
        'subcon_userid' : subcon_userid,
        'subcon_leads_id' : subcon_leads_id,
        'subcon_work_days' : subcon_work_days,
        'subcon_starting_date' : subcon_starting_date,
        'subcon_working_days' : subcon_working_days,
        'subcon_client_working_hours' : subcon_client_working_hours,
        'subcon_staff_timezone' : subcon_staff_timezone,
        'date_search' : [now.strftime('%Y-%m-%d')],
        'subcon_client_currency' : subcon_client_currency,
        'currencies' : currencies,
    }
    
    doc_id = col.insert(record)
    
    #retrieve a random document - READ
    #doc = col.find_one()
    #doc_id = '%s' % doc['_id']   
    #return doc_id
    

    query = "INSERT INTO mongo_staff_daily_attendance(mongodb_doc_id, search_date, date_created, status) VALUES('%s', '%s', '%s', '%s');" % (doc_id, '%s' % now.strftime('%Y-%m-%d'), '%s' % get_ph_time(), 'waiting')
    mongo_staff_daily_attendance_id = conn.execute(query).lastrowid 
    conn.close()
    
    
    #send_task("get_staff_daily_attendance_result.process", [mongo_staff_daily_attendance_id])
    get_staff_daily_attendance_result.process(mongo_staff_daily_attendance_id)
    #return doc_id
    #return mongo_staff_daily_attendance_id
				
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process('2013-05-16'))