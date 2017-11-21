#!/usr/bin/env python
#   2013-05-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   disabled skype messaging
#   2013-05-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add task to expire personal.mass_responder_code
#   -   remind norman on updates of this file
#   2013-04-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial commit for scheduled celery task of updating subcontractors.client_price


from sqlalchemy.sql import text
from persistent_mysql_connection import engine

from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

from decimal import Decimal


import couchdb

#import locale
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import settings


from pymongo import MongoClient
from bson.objectid import ObjectId

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

from pymongo import MongoClient
from bson.objectid import ObjectId

PHTZ = timezone('Asia/Manila')

couch_server = couchdb.Server(settings.COUCH_DSN)
couch_mailbox = couch_server['mailbox']

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
def staff_work_schedule_update(subcon_id):
    logging.info('executing celery task schedule.staff_work_schedule_update %s' % subcon_id)        
    #send notice to devs
    now = get_ph_time(as_array=False) 
    to=['devs@remotestaff.com.au']
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]  		
    mailbox = dict(
        sent = False,		
        bcc = None,
        cc = None,
        created = date_created,
        generated_by = 'celery schedule.staff_work_schedule_update',
        html = None,
        text = 'executing celery task schedule.staff_work_schedule_update %s' % subcon_id,		
        subject = 'executing celery task schedule.staff_work_schedule_update %s' % subcon_id,
        to = to			
    )
    mailbox['from'] = 'noreply@remotestaff.com.au'		
    couch_mailbox.save(mailbox)
    
    conn = engine.connect()
    #get the schedule setting	
    sql = text("""SELECT id, admin_id, working_hours, working_days, work_days, work_status,
                    mon_start, mon_finish, mon_number_hrs, mon_start_lunch, mon_finish_lunch, mon_lunch_number_hrs, 
                    tue_start, tue_finish, tue_number_hrs, tue_start_lunch, tue_finish_lunch, tue_lunch_number_hrs, 
                    wed_start, wed_finish, wed_number_hrs, wed_start_lunch, wed_finish_lunch, wed_lunch_number_hrs, 
                    thu_start, thu_finish, thu_number_hrs, thu_start_lunch, thu_finish_lunch, thu_lunch_number_hrs, 
                    fri_start, fri_finish, fri_number_hrs, fri_start_lunch, fri_finish_lunch, fri_lunch_number_hrs, 
                    sat_start, sat_finish, sat_number_hrs, sat_start_lunch, sat_finish_lunch, sat_lunch_number_hrs, 
                    sun_start, sun_finish, sun_number_hrs, sun_start_lunch, sun_finish_lunch, sun_lunch_number_hrs 
                    FROM subcontractors_scheduled_working_time WHERE status='waiting' AND subcon_id=:subcon_id""")	
    scheduled = conn.execute(sql, subcon_id=subcon_id).fetchone()
    #return scheduled.id
    str=""
    if scheduled:
        #Get the current data of subcon
        sql = text("""SELECT leads_id, working_hours, working_days, work_days, work_status,
                    mon_start, mon_finish, mon_number_hrs, mon_start_lunch, mon_finish_lunch, mon_lunch_number_hrs, 
                    tue_start, tue_finish, tue_number_hrs, tue_start_lunch, tue_finish_lunch, tue_lunch_number_hrs, 
                    wed_start, wed_finish, wed_number_hrs, wed_start_lunch, wed_finish_lunch, wed_lunch_number_hrs, 
                    thu_start, thu_finish, thu_number_hrs, thu_start_lunch, thu_finish_lunch, thu_lunch_number_hrs, 
                    fri_start, fri_finish, fri_number_hrs, fri_start_lunch, fri_finish_lunch, fri_lunch_number_hrs, 
                    sat_start, sat_finish, sat_number_hrs, sat_start_lunch, sat_finish_lunch, sat_lunch_number_hrs, 
                    sun_start, sun_finish, sun_number_hrs, sun_start_lunch, sun_finish_lunch, sun_lunch_number_hrs
                    FROM subcontractors WHERE id=:subcon_id""")
        subcon = conn.execute(sql, subcon_id=subcon_id).fetchone()
        #str = '\n%s' % subcon.mon_start
        
        if '%s' % subcon.working_hours != '%s' % scheduled.working_hours:
            str += "Working Hours from %s to %s<br>" % ('%s' % subcon.working_hours, '%s' % scheduled.working_hours)
                        
        if '%s' % subcon.working_days != '%s' % scheduled.working_days:
            str += 'No. Work Days from %s to %s<br>' % ('%s' % subcon.working_days, '%s' % scheduled.working_days)

        if '%s' % subcon.work_days != '%s' % scheduled.work_days:
            str += 'Working Days from %s to %s<br>' % ('%s' % subcon.work_days, '%s' % scheduled.work_days)
            
        if '%s' % subcon.work_status != '%s' % scheduled.work_status:
            str += 'Working Status from %s to %s<br>' % ('%s' % subcon.work_status, '%s' % scheduled.work_status)    

        #Monday    
        if '%s' % subcon.mon_start != '%s' % scheduled.mon_start:
            str += 'Monday Start Work from %s to %s<br>' % ('%s' % subcon.mon_start, '%s' % scheduled.mon_start)

        if '%s' % subcon.mon_finish != '%s' % scheduled.mon_finish:
            str += 'Monday Finish Work from %s to %s<br>' % ('%s' % subcon.mon_finish, '%s' % scheduled.mon_finish)        
        
        if '%s' % subcon.mon_number_hrs != '%s' % scheduled.mon_number_hrs:
            str += 'Monday Work Hours from %s to %s<br>' % ('%s' % subcon.mon_number_hrs, '%s' % scheduled.mon_number_hrs)
            
        if '%s' % subcon.mon_start_lunch != '%s' % scheduled.mon_start_lunch:
            str += 'Monday Start Lunch from %s to %s<br>' % ('%s' % subcon.mon_start_lunch, '%s' % scheduled.mon_start_lunch)
        
        if '%s' % subcon.mon_finish_lunch != '%s' % scheduled.mon_finish_lunch:
            str += 'Monday Finish Lunch from %s to %s<br>' % ('%s' % subcon.mon_finish_lunch, '%s' % scheduled.mon_finish_lunch)
        
        if '%s' % subcon.mon_lunch_number_hrs != '%s' % scheduled.mon_lunch_number_hrs:
            str += 'Monday Lunch Hours from %s to %s<br>' % ('%s' % subcon.mon_lunch_number_hrs, '%s' % scheduled.mon_lunch_number_hrs)
            
            
        #Tuesday    
        if '%s' % subcon.tue_start != '%s' % scheduled.tue_start:
            str += 'Tuesday Start Work from %s to %s<br>' % ('%s' % subcon.tue_start, '%s' % scheduled.tue_start)

        if '%s' % subcon.tue_finish != '%s' % scheduled.tue_finish:
            str += 'Tuesday Finish Work from %s to %s<br>' % ('%s' % subcon.tue_finish, '%s' % scheduled.tue_finish)        
        
        if '%s' % subcon.tue_number_hrs != '%s' % scheduled.tue_number_hrs:
            str += 'Tuesday Work Hours from %s to %s<br>' % ('%s' % subcon.tue_number_hrs, '%s' % scheduled.tue_number_hrs)
            
        if '%s' % subcon.tue_start_lunch != '%s' % scheduled.tue_start_lunch:
            str += 'Tuesday Start Lunch from %s to %s<br>' % ('%s' % subcon.tue_start_lunch, '%s' % scheduled.tue_start_lunch)
        
        if '%s' % subcon.tue_finish_lunch != '%s' % scheduled.tue_finish_lunch:
            str += 'Tuesday Finish Lunch from %s to %s<br>' % ('%s' % subcon.tue_finish_lunch, '%s' % scheduled.tue_finish_lunch)
        
        if '%s' % subcon.tue_lunch_number_hrs != '%s' % scheduled.tue_lunch_number_hrs:
            str += 'Tuesday Lunch Hours from %s to %s<br>' % ('%s' % subcon.tue_lunch_number_hrs, '%s' % scheduled.tue_lunch_number_hrs)


        #Wednesday    
        if '%s' % subcon.wed_start != '%s' % scheduled.wed_start:
            str += 'Wednesday Start Work from %s to %s<br>' % ('%s' % subcon.wed_start, '%s' % scheduled.wed_start)

        if '%s' % subcon.wed_finish != '%s' % scheduled.wed_finish:
            str += 'Wednesday Finish Work from %s to %s<br>' % ('%s' % subcon.wed_finish, '%s' % scheduled.wed_finish)        
        
        if '%s' % subcon.wed_number_hrs != '%s' % scheduled.wed_number_hrs:
            str += 'Wednesday Work Hours from %s to %s<br>' % ('%s' % subcon.wed_number_hrs, '%s' % scheduled.wed_number_hrs)
            
        if '%s' % subcon.wed_start_lunch != '%s' % scheduled.wed_start_lunch:
            str += 'Wednesday Start Lunch from %s to %s<br>' % ('%s' % subcon.wed_start_lunch, '%s' % scheduled.wed_start_lunch)
        
        if '%s' % subcon.wed_finish_lunch != '%s' % scheduled.wed_finish_lunch:
            str += 'Wednesday Finish Lunch from %s to %s<br>' % ('%s' % subcon.wed_finish_lunch, '%s' % scheduled.wed_finish_lunch)
        
        if '%s' % subcon.wed_lunch_number_hrs != '%s' % scheduled.wed_lunch_number_hrs:
            str += 'Wednesday Lunch Hours from %s to %s<br>' % ('%s' % subcon.wed_lunch_number_hrs, '%s' % scheduled.wed_lunch_number_hrs)


        #Thursday    
        if '%s' % subcon.thu_start != '%s' % scheduled.thu_start:
            str += 'Thursday Start Work from %s to %s<br>' % ('%s' % subcon.thu_start, '%s' % scheduled.thu_start)

        if '%s' % subcon.thu_finish != '%s' % scheduled.thu_finish:
            str += 'Thursday Finish Work from %s to %s<br>' % ('%s' % subcon.thu_finish, '%s' % scheduled.thu_finish)        
        
        if '%s' % subcon.thu_number_hrs != '%s' % scheduled.thu_number_hrs:
            str += 'Thursday Work Hours from %s to %s<br>' % ('%s' % subcon.thu_number_hrs, '%s' % scheduled.thu_number_hrs)
            
        if '%s' % subcon.thu_start_lunch != '%s' % scheduled.thu_start_lunch:
            str += 'Thursday Start Lunch from %s to %s<br>' % ('%s' % subcon.thu_start_lunch, '%s' % scheduled.thu_start_lunch)
        
        if '%s' % subcon.thu_finish_lunch != '%s' % scheduled.thu_finish_lunch:
            str += 'Thursday Finish Lunch from %s to %s<br>' % ('%s' % subcon.thu_finish_lunch, '%s' % scheduled.thu_finish_lunch)
        
        if '%s' % subcon.thu_lunch_number_hrs != '%s' % scheduled.thu_lunch_number_hrs:
            str += 'Thursday Lunch Hours from %s to %s<br>' % ('%s' % subcon.thu_lunch_number_hrs, '%s' % scheduled.thu_lunch_number_hrs) 


        #Friday    
        if '%s' % subcon.fri_start != '%s' % scheduled.fri_start:
            str += 'Friday Start Work from %s to %s<br>' % ('%s' % subcon.fri_start, '%s' % scheduled.fri_start)

        if '%s' % subcon.fri_finish != '%s' % scheduled.fri_finish:
            str += 'Friday Finish Work from %s to %s<br>' % ('%s' % subcon.fri_finish, '%s' % scheduled.fri_finish)        
        
        if '%s' % subcon.fri_number_hrs != '%s' % scheduled.fri_number_hrs:
            str += 'Friday Work Hours from %s to %s<br>' % ('%s' % subcon.fri_number_hrs, '%s' % scheduled.fri_number_hrs)
            
        if '%s' % subcon.fri_start_lunch != '%s' % scheduled.fri_start_lunch:
            str += 'Friday Start Lunch from %s to %s<br>' % ('%s' % subcon.fri_start_lunch, '%s' % scheduled.fri_start_lunch)
        
        if '%s' % subcon.fri_finish_lunch != '%s' % scheduled.fri_finish_lunch:
            str += 'Friday Finish Lunch from %s to %s<br>' % ('%s' % subcon.fri_finish_lunch, '%s' % scheduled.fri_finish_lunch)
        
        if '%s' % subcon.fri_lunch_number_hrs != '%s' % scheduled.fri_lunch_number_hrs:
            str += 'Friday Lunch Hours from %s to %s<br>' % ('%s' % subcon.fri_lunch_number_hrs, '%s' % scheduled.fri_lunch_number_hrs)


        #Saturday    
        if '%s' % subcon.sat_start != '%s' % scheduled.sat_start:
            str += 'Saturday Start Work from %s to %s<br>' % ('%s' % subcon.sat_start, '%s' % scheduled.sat_start)

        if '%s' % subcon.sat_finish != '%s' % scheduled.sat_finish:
            str += 'Saturday Finish Work from %s to %s<br>' % ('%s' % subcon.sat_finish, '%s' % scheduled.sat_finish)        
        
        if '%s' % subcon.sat_number_hrs != '%s' % scheduled.sat_number_hrs:
            str += 'Saturday Work Hours from %s to %s<br>' % ('%s' % subcon.sat_number_hrs, '%s' % scheduled.sat_number_hrs)
            
        if '%s' % subcon.sat_start_lunch != '%s' % scheduled.sat_start_lunch:
            str += 'Saturday Start Lunch from %s to %s<br>' % ('%s' % subcon.sat_start_lunch, '%s' % scheduled.sat_start_lunch)
        
        if '%s' % subcon.sat_finish_lunch != '%s' % scheduled.sat_finish_lunch:
            str += 'Saturday Finish Lunch from %s to %s<br>' % ('%s' % subcon.sat_finish_lunch, '%s' % scheduled.sat_finish_lunch)
        
        if '%s' % subcon.sat_lunch_number_hrs != '%s' % scheduled.sat_lunch_number_hrs:
            str += 'Saturday Lunch Hours from %s to %s<br>' % ('%s' % subcon.sat_lunch_number_hrs, '%s' % scheduled.sat_lunch_number_hrs)   


        #Sunday    
        if '%s' % subcon.sun_start != '%s' % scheduled.sun_start:
            str += 'Sunday Start Work from %s to %s<br>' % ('%s' % subcon.sun_start, '%s' % scheduled.sun_start)

        if '%s' % subcon.sun_finish != '%s' % scheduled.sun_finish:
            str += 'Sunday Finish Work from %s to %s<br>' % ('%s' % subcon.sun_finish, '%s' % scheduled.sun_finish)        
        
        if '%s' % subcon.sun_number_hrs != '%s' % scheduled.sun_number_hrs:
            str += 'Sunday Work Hours from %s to %s<br>' % ('%s' % subcon.sun_number_hrs, '%s' % scheduled.sun_number_hrs)
            
        if '%s' % subcon.sun_start_lunch != '%s' % scheduled.sun_start_lunch:
            str += 'Sunday Start Lunch from %s to %s<br>' % ('%s' % subcon.sun_start_lunch, '%s' % scheduled.sun_start_lunch)
        
        if '%s' % subcon.sun_finish_lunch != '%s' % scheduled.sun_finish_lunch:
            str += 'Sunday Finish Lunch from %s to %s<br>' % ('%s' % subcon.sun_finish_lunch, '%s' % scheduled.sun_finish_lunch)
        
        if '%s' % subcon.sun_lunch_number_hrs != '%s' % scheduled.sun_lunch_number_hrs:
            str += 'Sunday Lunch Hours from %s to %s<br>' % ('%s' % subcon.sun_lunch_number_hrs, '%s' % scheduled.sun_lunch_number_hrs)              
        
        
        #update subcontractors table
        sql = text("""
            UPDATE subcontractors SET 
            working_hours = :working_hours, 
            working_days = :working_days, 
            work_days = :work_days, 
            work_status = :work_status,
            mon_start = :mon_start, 
            mon_finish = :mon_finish, 
            mon_number_hrs = :mon_number_hrs, 
            mon_start_lunch = :mon_start_lunch, 
            mon_finish_lunch = :mon_finish_lunch, 
            mon_lunch_number_hrs = :mon_lunch_number_hrs, 
            tue_start = :tue_start, 
            tue_finish = :tue_finish, 
            tue_number_hrs = :tue_number_hrs, 
            tue_start_lunch = :tue_start_lunch, 
            tue_finish_lunch = :tue_finish_lunch, 
            tue_lunch_number_hrs = :tue_lunch_number_hrs, 
            wed_start = :wed_start, 
            wed_finish = :wed_finish, 
            wed_number_hrs = :wed_number_hrs, 
            wed_start_lunch = :wed_start_lunch, 
            wed_finish_lunch = :wed_finish_lunch, 
            wed_lunch_number_hrs = :wed_lunch_number_hrs, 
            thu_start = :thu_start, 
            thu_finish = :thu_finish, 
            thu_number_hrs = :thu_number_hrs, 
            thu_start_lunch = :thu_start_lunch, 
            thu_finish_lunch = :thu_finish_lunch, 
            thu_lunch_number_hrs = :thu_lunch_number_hrs, 
            fri_start = :fri_start, 
            fri_finish = :fri_finish, 
            fri_number_hrs = :fri_number_hrs, 
            fri_start_lunch = :fri_start_lunch, 
            fri_finish_lunch = :fri_finish_lunch, 
            fri_lunch_number_hrs = :fri_lunch_number_hrs, 
            sat_start = :sat_start, 
            sat_finish = :sat_finish, 
            sat_number_hrs = :sat_number_hrs, 
            sat_start_lunch = :sat_start_lunch, 
            sat_finish_lunch = :sat_finish_lunch, 
            sat_lunch_number_hrs = :sat_lunch_number_hrs, 
            sun_start = :sun_start, 
            sun_finish = :sun_finish, 
            sun_number_hrs = :sun_number_hrs, 
            sun_start_lunch = :sun_start_lunch, 
            sun_finish_lunch = :sun_finish_lunch, 
            sun_lunch_number_hrs = :sun_lunch_number_hrs
            WHERE id = :subcon_id
        """)
        now = get_ph_time()
        client_price_effective_date = date(now.year, now.month, now.day)
        conn.execute(
            sql, 
            subcon_id = subcon_id, 
            working_hours = scheduled.working_hours, 
            working_days = scheduled.working_days, 
            work_days = scheduled.work_days,
            work_status = scheduled.work_status,    
            mon_start = scheduled.mon_start, 
            mon_finish = scheduled.mon_finish, 
            mon_number_hrs = scheduled.mon_number_hrs, 
            mon_start_lunch = scheduled.mon_start_lunch, 
            mon_finish_lunch = scheduled.mon_finish_lunch, 
            mon_lunch_number_hrs = scheduled.mon_lunch_number_hrs, 
            tue_start = scheduled.tue_start, 
            tue_finish = scheduled.tue_finish, 
            tue_number_hrs = scheduled.tue_number_hrs, 
            tue_start_lunch = scheduled.tue_start_lunch, 
            tue_finish_lunch = scheduled.tue_finish_lunch, 
            tue_lunch_number_hrs = scheduled.tue_lunch_number_hrs, 
            wed_start = scheduled.wed_start, 
            wed_finish = scheduled.wed_finish, 
            wed_number_hrs = scheduled.wed_number_hrs, 
            wed_start_lunch = scheduled.wed_start_lunch, 
            wed_finish_lunch = scheduled.wed_finish_lunch, 
            wed_lunch_number_hrs = scheduled.wed_lunch_number_hrs, 
            thu_start = scheduled.thu_start, 
            thu_finish = scheduled.thu_finish, 
            thu_number_hrs = scheduled.thu_number_hrs, 
            thu_start_lunch = scheduled.thu_start_lunch, 
            thu_finish_lunch = scheduled.thu_finish_lunch, 
            thu_lunch_number_hrs = scheduled.thu_lunch_number_hrs, 
            fri_start = scheduled.fri_start, 
            fri_finish = scheduled.fri_finish, 
            fri_number_hrs = scheduled.fri_number_hrs, 
            fri_start_lunch = scheduled.fri_start_lunch, 
            fri_finish_lunch = scheduled.fri_finish_lunch, 
            fri_lunch_number_hrs = scheduled.fri_lunch_number_hrs, 
            sat_start = scheduled.sat_start, 
            sat_finish = scheduled.sat_finish, 
            sat_number_hrs = scheduled.sat_number_hrs, 
            sat_start_lunch = scheduled.sat_start_lunch, 
            sat_finish_lunch = scheduled.sat_finish_lunch, 
            sat_lunch_number_hrs = scheduled.sat_lunch_number_hrs, 
            sun_start = scheduled.sun_start, 
            sun_finish = scheduled.sun_finish, 
            sun_number_hrs = scheduled.sun_number_hrs, 
            sun_start_lunch = scheduled.sun_start_lunch, 
            sun_finish_lunch = scheduled.sun_finish_lunch, 
            sun_lunch_number_hrs = scheduled.sun_lunch_number_hrs
        )
        
        
        #mag add ng history sa subcontractors_history
        sql = text("""
            INSERT INTO subcontractors_history
            (subcontractors_id, date_change, changes, change_by_id, changes_status, note)
            VALUES (:subcontractors_id, :date_change, :changes, :change_by_id, :changes_status, :note)
        """)
        conn.execute(sql,
            subcontractors_id = subcon_id,
            date_change = now,
            changes = 'SYSTEM EXECUTED SCHEDULED WORKING TIME UPDATES.<br><b>Changes made : </b><br>%s' % str,
            change_by_id = 5,
            change_by_type = 'admin',			
            changes_status = 'approved',
            note = 'Executed via celery'		
        )
        
        #need i-update yung subcontractors_scheduled_working_time
        sql = text("""UPDATE subcontractors_scheduled_working_time SET status = 'executed', date_executed=:date_executed WHERE id = :scheduled_id""")
        conn.execute(sql, scheduled_id = scheduled.id, date_executed=now)
        
        
        #Send email		
		#set up the recipients
        recipients=['admin@remotestaff.com.au']
		
		#admin who scheduled the update  => scheduled.admin_id
        sql = text("""SELECT admin_email FROM admin WHERE admin_id=:admin_id""")
        admin = conn.execute(sql, admin_id=scheduled.admin_id).fetchone()
        recipients.append(admin.admin_email)  		
        
		#client csro
        sql = text("""SELECT csro_id FROM leads WHERE id=:leads_id""")
        c = conn.execute(sql, leads_id=subcon.leads_id).fetchone()
        if c:
            sql = text("""SELECT admin_email FROM admin WHERE admin_id=:admin_id""")
            csro = conn.execute(sql, admin_id=c.csro_id).fetchone()
            if csro.admin_email:			
                recipients.append(csro.admin_email)
                
        #save email messasge in couchdb mailbox
        html_message = "<p>Executed scheduled <strong>Working Time</strong> updates for contract #%s<br><small>via celery</small></p>%s" % (subcon_id, str)
        to=['admin@remotestaff.com.au']		
        cc=[]
        cc.append(admin.admin_email)
        if c:
            cc.append(csro.admin_email)        		
        bcc=['devs@remotestaff.com.au']
        date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]  		
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = date_created,
            generated_by = 'celery schedule.staff_work_schedule_update',
            html = html_message,
            subject = 'Staff Working Time Updated for Contract #%s' % subcon_id,
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)
        
    conn.close()
    #return str
    

@task(ignore_result=True)
def client_price_update(subcon_id, client_price):
    #send_task('skype_messaging.notify_devs', ['Executing schedule.client_price_update(%s, %s)' % (subcon_id, client_price)])
    logging.info('Executing schedule.client_price_update(%s, %s)' % (subcon_id, client_price))
    conn = engine.connect()
	
    #get the schedule setting	
    sql = text("""SELECT id, scheduled_date, rate, work_status, added_by_id FROM subcontractors_scheduled_client_rate WHERE status='waiting' AND subcontractors_id=:subcon_id""")	
    s = conn.execute(sql, subcon_id=subcon_id).fetchone()

    if s:	
        client_price = s.rate	
        	
        #Get the current client_price and client_price_effective_date
        sql = text("""SELECT leads_id, client_price, client_price_effective_date, work_status FROM subcontractors WHERE id=:subcon_id""")
        r = conn.execute(sql, subcon_id=subcon_id).fetchone()

        str=""
		
        #pending new hourly rate		
        if s.work_status == 'Part-Time' :	
            client_price_hourly = ((((float(client_price) * 12 ) / 52 ) / 5 ) / 4 )		
        else :
            client_price_hourly = ((((float(client_price) * 12 ) / 52 ) / 5 ) / 8 )

        #previous hourly rate
        if r.work_status == 'Part-Time' :	
            current_client_price_hourly = ((((float(r.client_price) * 12 ) / 52 ) / 5 ) / 4 )		
        else :
            current_client_price_hourly = ((((float(r.client_price) * 12 ) / 52 ) / 5 ) / 8 )			
		
        client_price_hourly = '%0.2f' % client_price_hourly
        current_client_price_hourly	= '%0.2f' % current_client_price_hourly
        client_price = '%0.2f' % client_price		
	
        if ('%s' % r.client_price) != ('%s' % client_price):
            str += 'CLIENT QUOTED PRICE from %s to %s<br>' % (r.client_price, client_price)	
        if ('%s' % current_client_price_hourly) != ('%s' % client_price_hourly):        	
            str += 'CLIENT HOURLY RATE from %s to %s<br>' % (current_client_price_hourly, client_price_hourly)
        if ('%s' % r.client_price_effective_date) != ('%s' % s.scheduled_date.strftime('%Y-%m-%d')):        	
            str += 'EFFECTIVE DATE OF THE NEW CLIENT PRICE from %s to %s<br>' % (r.client_price_effective_date, s.scheduled_date.strftime('%Y-%m-%d'))		
	
        #update subcontractors table
        sql = text("""
            UPDATE subcontractors
            SET client_price = :client_price,
            client_price_effective_date = :client_price_effective_date
            WHERE id = :subcon_id
        """)
        now = get_ph_time()
        client_price_effective_date = date(now.year, now.month, now.day)
        conn.execute(sql, 
            subcon_id = subcon_id, 
            client_price = client_price, 
            client_price_effective_date = client_price_effective_date)

        #mag add ng history sa subcontractors_history
        sql = text("""
            INSERT INTO subcontractors_history
            (subcontractors_id, date_change, changes, change_by_id, changes_status, note)
            VALUES (:subcontractors_id, :date_change, :changes, :change_by_id, :changes_status, :note)
        """)
        result = conn.execute(sql,
            subcontractors_id = subcon_id,
            date_change = now,
            changes = 'SYSTEM EXECUTED SCHEDULED CLIENT PRICE UPDATES.<br><b>Changes made : </b><br>%s' % str,
            change_by_id = 5,
            change_by_type = 'admin',			
            changes_status = 'approved',
            note = 'Executed via celery'		
        )
        history_id = result.lastrowid

        #update first the end_date of the current rate 
        sql = text("""UPDATE subcontractors_client_rate SET end_date = :end_date, work_status = :work_status WHERE end_date IS NULL AND subcontractors_id = :subcon_id""")
        conn.execute(sql, 
            subcon_id = subcon_id, 
            end_date = client_price_effective_date,
            work_status = r.work_status			
        )     	
	
        #insert new record sa subcontractors_client_rate
        now = get_ph_time()		
        sql = text("""
            INSERT INTO subcontractors_client_rate
            (subcontractors_id, start_date, rate, work_status, date_added)
            VALUES (:subcontractors_id, :start_date, :rate, :work_status, :date_added)
        """)
        result = conn.execute(sql,
            subcontractors_id = subcon_id,
            start_date = s.scheduled_date,
            rate = client_price, 
            work_status = s.work_status,
            date_added = now		
        )	
        subcontractors_client_rate_id = result.lastrowid
        
        if settings.DEBUG:
            mongo_client = MongoClient(host=settings.MONGO_TEST)
        else:
            mongo_client = MongoClient(host=settings.MONGO_PROD , port=27017)
            
        db = mongo_client.subcontractors
        col = db.update_rates_comments
        doc = col.find_one(  {"subcontractors_id" : int(subcon_id),  "subcontractors_scheduled_client_rate_id" : int(s.id) }  )    
        if doc:
            #col.update({"_id" : doc["_id"]}, {"$set":{'status' : 'executed'}})
            record={
                'history_id' : int(history_id),    
                'subcontractors_id' : int(subcon_id),
                'comment_note' : '%s' % doc["comment_note"],
                'admin_id' : doc["admin_id"],
                'admin_name' : doc["admin_name"],
                'date_search' : now,
                'status' : 'executed',
                'subcontractors_client_rate_id' : int(subcontractors_client_rate_id),
                'reference_mongo_id' : doc["_id"]
            }
            doc_id = col.insert(record)    
            sql = text("""UPDATE subcontractors_history SET note= :comment_note WHERE id = :history_id""")
            conn.execute(sql, history_id = history_id, comment_note = '%s' % doc["comment_note"])
           
        #need i-update yung subcontractors_scheduled_client_rate
        sql = text("""UPDATE subcontractors_scheduled_client_rate SET status = 'executed' WHERE status='waiting' AND subcontractors_id = :subcon_id""")
        conn.execute(sql, subcon_id = subcon_id)	
        
		
		
        #Send email		
		#set up the recipients
        recipients=['admin@remotestaff.com.au']
		
		#admin who scheduled the update  => s.added_by_id
        sql = text("""SELECT admin_email FROM admin WHERE admin_id=:admin_id""")
        admin = conn.execute(sql, admin_id=s.added_by_id).fetchone()
        recipients.append(admin.admin_email)  		
		#client csro
        sql = text("""SELECT csro_id FROM leads WHERE id=:leads_id""")
        c = conn.execute(sql, leads_id=r.leads_id).fetchone()
        if c:
            sql = text("""SELECT admin_email FROM admin WHERE admin_id=:admin_id""")
            csro = conn.execute(sql, admin_id=c.csro_id).fetchone()
            if csro.admin_email:			
                recipients.append(csro.admin_email)		
		
        #save email messasge in couchdb mailbox
        html_message = "<p>Executed scheduled <strong>client price</strong> updates for contract #%s<br><small>via celery</small></p>" % subcon_id
        to=['admin@remotestaff.com.au']		
        cc=[]
        cc.append(admin.admin_email)
        if c:
            cc.append(csro.admin_email)        		
        bcc=['devs@remotestaff.com.au']
		
        date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]  		
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = date_created,
            generated_by = 'celery schedule.client_price_update',
            html = html_message,
            subject = 'Client Price Updated for Contract #%s' % subcon_id,
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)
        #attach message
        #couch_mailbox.put_attachment(doc_order_archive, html_message, 'message.html')		
        #send_task('skype_messaging.notify_devs', ['Executed schedule.client_price_update(%s, %s)' % (subcon_id, client_price)])  
        
    #else:
        #send_task('skype_messaging.notify_devs', ['Failed Execution schedule.client_price_update(%s, %s). Check subcontractors_scheduled_client_rate table.' % (subcon_id, client_price)])    
        #print 'Execution Failed';
        
        
    conn.close()

@task(ignore_result=True)    
def staff_salary_update(subcon_id):
    logging.info('Executing schedule.staff_salary_update subcon_id => %s ' % subcon_id )
    conn = engine.connect()
	
    #get the schedule setting	
    sql = text("""SELECT id, scheduled_date, rate, work_status, status, added_by_id, added_by_type FROM subcontractors_scheduled_subcon_rate WHERE status='waiting' AND subcontractors_id=:subcon_id""")	
    s = conn.execute(sql, subcon_id=subcon_id).fetchone()
    
    str=""
    if s:	
        staff_salary = s.rate	
        	
        #Get the current client_price and client_price_effective_date
        sql = text("""SELECT leads_id, userid, php_monthly, php_hourly, work_status FROM subcontractors WHERE id=:subcon_id""")
        r = conn.execute(sql, subcon_id=subcon_id).fetchone()
      
        #pending new hourly rate		
        if s.work_status == 'Part-Time' :	
            staff_salary_hourly = ((((float(staff_salary) * 12 ) / 52 ) / 5 ) / 4 )		
        else :
            staff_salary_hourly = ((((float(staff_salary) * 12 ) / 52 ) / 5 ) / 8 )    
            
        staff_salary_hourly = '%0.2f' % staff_salary_hourly
        current_staff_salary_hourly	= '%0.2f' % r.php_hourly
        staff_salary = '%0.2f' % staff_salary
        
        
        if ('%s' % r.php_monthly) != ('%s' % staff_salary):
            str += 'STAFF MONTHLY SALARY from %s to %s<br>' % (r.php_monthly, staff_salary)	
            
        if ('%s' % current_staff_salary_hourly) != ('%s' % staff_salary_hourly):        	
            str += 'STAFF HOURLY SALARY from %s to %s<br>' % (current_staff_salary_hourly, staff_salary_hourly)
            
        #update subcontractors table
        sql = text("""
            UPDATE subcontractors
            SET php_monthly = :staff_salary,
            php_hourly = :staff_salary_hourly
            WHERE id = :subcon_id
        """)
        now = get_ph_time()
        
        conn.execute(sql, 
            subcon_id = subcon_id, 
            staff_salary = staff_salary, 
            staff_salary_hourly = staff_salary_hourly)
        
        #mag add ng history sa subcontractors_history
        sql = text("""
            INSERT INTO subcontractors_history
            (subcontractors_id, date_change, changes, change_by_id, changes_status, note)
            VALUES (:subcontractors_id, :date_change, :changes, :change_by_id, :changes_status, :note)
        """)
        result = conn.execute(sql,
            subcontractors_id = subcon_id,
            date_change = now,
            changes = 'SYSTEM EXECUTED SCHEDULED STAFF CONTRACT SALARY UPDATES.<br>%s' % str,
            change_by_id = 5,
            change_by_type = 'admin',			
            changes_status = 'approved',
            note = 'Executed via celery'		
        )    
        history_id = result.lastrowid
        
        
            
        #insert new record sa subcontractors_staff_rate
        now = get_ph_time()		
        sql = text("""
            INSERT INTO subcontractors_staff_rate
            (subcontractors_id, start_date, rate, work_status)
            VALUES (:subcontractors_id, :start_date, :rate, :work_status)
        """)
        result = conn.execute(sql,
            subcontractors_id = subcon_id,
            start_date = s.scheduled_date,
            rate = staff_salary, 
            work_status = s.work_status,
        )
        
        subcontractors_staff_rate_id = result.lastrowid
        
        if settings.DEBUG:
            mongo_client = MongoClient(host=settings.MONGO_TEST)
        else:
            mongo_client = MongoClient(host=settings.MONGO_PROD , port=27017)
        
        db = mongo_client.subcontractors
        col = db.update_rates_comments
        doc = col.find_one(  {"subcontractors_id" : int(subcon_id),  "subcontractors_scheduled_subcon_rate_id" : int(s.id)}  )
        if doc:
            #col.update({"_id" : doc["_id"]}, {"$set":{'status' : 'executed'}})
            record={
                'history_id' : int(history_id),    
                'subcontractors_id' : int(subcon_id),
                'comment_note' : '%s' % doc["comment_note"],
                'admin_id' : doc["admin_id"],
                'admin_name' : doc["admin_name"],
                'date_search' : now,
                'status' : 'executed',
                'subcontractors_staff_rate_id' : int(subcontractors_staff_rate_id),
                'reference_mongo_id' : doc["_id"]
            }
            doc_id = col.insert(record)
            sql = text("""UPDATE subcontractors_history SET note= :comment_note WHERE id = :history_id""")
            conn.execute(sql, history_id = history_id, comment_note = '%s' % doc["comment_note"])
            
        
        #need i-update yung subcontractors_scheduled_subcon_rate
        sql = text("""UPDATE subcontractors_scheduled_subcon_rate SET status = 'executed' WHERE status='waiting' AND subcontractors_id = :subcon_id""")
        conn.execute(sql, subcon_id = subcon_id)
        
        
        #Send email	
        
		#client csro
        sql = text("""SELECT csro_id FROM leads WHERE id=:leads_id""")
        c = conn.execute(sql, leads_id=r.leads_id).fetchone()
        if c:
            sql = text("""SELECT admin_email FROM admin WHERE admin_id=:admin_id""")
            csro = conn.execute(sql, admin_id=c.csro_id).fetchone()	
		
        #save email messasge in couchdb mailbox
        html_message = "<p>Executed scheduled <strong>staff salary</strong> updates for contract #%s<br><small>via celery</small></p>" % subcon_id
        to=['admin@remotestaff.com.au']		
        cc=[]
        #cc.append(admin.admin_email)
        if c:
            cc.append(csro.admin_email)        		
        bcc=['devs@remotestaff.com.au']
		
        date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]  		
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = date_created,
            generated_by = 'celery schedule.staff_salary_update',
            html = html_message,
            subject = 'Staff Salary Updated for Contract #%s' % subcon_id,
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)
        
        
    conn.close()
    
    return str


def run_tests():
    """
    >>> client_price_update(5413, '100000.00')
    >>> staff_salary_update(5397)
    >>> staff_work_schedule_update(4991)
    """
	

if __name__ == '__main__':
    import doctest
    doctest.testmod()