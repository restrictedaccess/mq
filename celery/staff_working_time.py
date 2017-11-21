#!/usr/bin/env python
#   2014-04-14  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   initial commit for automatic celery task of updating subcontractors working time due to dst time


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
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import settings

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
def dst_time_update(subcon_ids):
    send_task('skype_messaging.notify_devs', ['Executing staff_working_time.dst_time_update(%s)' % subcon_ids]) 
	
    conn = engine.connect()
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db = couch_server['mailbox']

	
    if subcon_ids:
        subcon_id_str=""	
        for s in subcon_ids:
            subcon_id_str +='%d,' % s	
			
        #print subcon_id_str[:-1]
        sql = "SELECT id, work_days, client_timezone, staff_working_timezone, client_start_work_hour, client_finish_work_hour FROM subcontractors WHERE id IN(%s)" % subcon_id_str[:-1]
        subcons = conn.execute(sql).fetchall()	
	
        work_days = ['mon', 'tue', 'wed', 'thu', 'fri']
        ph_tz = timezone('Asia/Manila')
        now = get_ph_time()
        str=""
	
        for	s in subcons:
            #print s.id	
            str +='<li>%s</li>' % s.id
            if s.work_days:
                work_days = s.work_days.split(',')
            working_days=[]
            for w in work_days:
                working_days.append('%s' % w)
			
            #client timezone
            client_tz = timezone(s.client_timezone)
            ph_tz = timezone(s.staff_working_timezone)  			
            client_start_work_hour = s.client_start_work_hour
            client_finish_work_hour = s.client_finish_work_hour

            #create datetime object			
            client_start_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), client_start_work_hour)
            client_start_work_hour = datetime.strptime(client_start_work_hour, '%Y-%m-%d %H:%M:%S')

            client_finish_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), client_finish_work_hour)
            client_finish_work_hour = datetime.strptime(client_finish_work_hour, '%Y-%m-%d %H:%M:%S')			

            #convert it to staff timezone
            staff_start_work_hour = client_tz.localize(client_start_work_hour).astimezone(ph_tz)
            staff_finish_work_hour = client_tz.localize(client_finish_work_hour).astimezone(ph_tz)			
            
            #check staff working days
            mon_start = None
            mon_finish = None
			
            tue_start = None
            tue_finish = None

            wed_start = None
            wed_finish = None
   
            thu_start = None
            thu_finish = None

            fri_start = None
            fri_finish = None

            sat_start = None
            sat_finish = None

            sun_start = None
            sun_finish = None 			
            
            			
            if 'mon' in	working_days:
                mon_start = staff_start_work_hour.strftime('%H:%M:%S')
                mon_finish = staff_finish_work_hour.strftime('%H:%M:%S')

            if 'tue' in	working_days:
                tue_start = staff_start_work_hour.strftime('%H:%M:%S')
                tue_finish = staff_finish_work_hour.strftime('%H:%M:%S')

            if 'wed' in	working_days:
                wed_start = staff_start_work_hour.strftime('%H:%M:%S')
                wed_finish = staff_finish_work_hour.strftime('%H:%M:%S')			
     
            if 'thu' in	working_days:
                thu_start = staff_start_work_hour.strftime('%H:%M:%S')
                thu_finish = staff_finish_work_hour.strftime('%H:%M:%S')
				
            if 'fri' in	working_days:
                fri_start = staff_start_work_hour.strftime('%H:%M:%S')
                fri_finish = staff_finish_work_hour.strftime('%H:%M:%S')				
				 
            if 'sat' in	working_days:
                sat_start = staff_start_work_hour.strftime('%H:%M:%S')
                sat_finish = staff_finish_work_hour.strftime('%H:%M:%S')

            if 'sun' in	working_days:
                sun_start = staff_start_work_hour.strftime('%H:%M:%S')
                sun_finish = staff_finish_work_hour.strftime('%H:%M:%S')		
				
            subcon_id = int(s.id)      			
            #print '%s | %s\n%s | %s' % (client_start_work_hour.strftime('%I:%M:%S'), client_finish_work_hour.strftime('%I:%M:%S'), staff_start_work_hour.strftime('%I:%M:%S'), staff_finish_work_hour.strftime('%I:%M:%S'))
            
            sql = text("""UPDATE subcontractors SET 
                mon_start = :mon_start, 
                mon_finish = :mon_finish, 
                tue_start = :tue_start, 
                tue_finish = :tue_finish, 
                wed_start = :wed_start, 
                wed_finish = :wed_finish, 
                thu_start = :thu_start, 
                thu_finish = :thu_finish, 
                fri_start = :fri_start, 
                fri_finish = :fri_finish, 
                sat_start = :sat_start, 
                sat_finish = :sat_finish, 
                sun_start = :sun_start, 
                sun_finish = :sun_finish 
                WHERE id = :subcon_id
            """)
            #print sql
            conn.execute(sql, 
                subcon_id = subcon_id, 
                mon_start = mon_start,
                mon_finish = mon_finish,
                tue_start = tue_start,
                tue_finish = tue_finish,
                wed_start = wed_start,
                wed_finish = wed_finish,
                thu_start = thu_start,
                thu_finish = thu_finish,
                fri_start = fri_start,
                fri_finish = fri_finish,
                sat_start = sat_start,
                sat_finish = sat_finish,
                sun_start = sun_start,
                sun_finish = sun_finish)
    			
            #mag add ng history sa subcontractors_history
            sql = text("""
                INSERT INTO subcontractors_history
                (subcontractors_id, date_change, changes, change_by_id, changes_status, note)
                VALUES (:subcontractors_id, :date_change, :changes, :change_by_id, :changes_status, :note)
            """)
            conn.execute(sql,
                subcontractors_id = subcon_id,
                date_change = now,
                changes = 'SYSTEM AUTOMATIC DST TIME UPDATES.<br>',
                change_by_id = 5, 
                change_by_type = 'admin',				 
                changes_status = 'approved',
                note = 'Executed via celery'		
            )
        #save email messasge in couchdb mailbox
        html_message = "<p><strong>Executed staff working time dst updates.</strong></p>"
        html_message += "<p>Involved Contract Ids:</p>"
        html_message += "<pre><ol>"
        html_message += "%s" % str		
        html_message += "</ol></pre>" 		
        to=['devs@remotestaff.com.au']		
        cc=[]        		
        bcc=[]
		
        date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]  		
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = date_created,
            generated_by = 'celery staff_working_time.dst_time_update',
            html = html_message,
            subject = 'Staff Contracts DST Updates',
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)			
        send_task('skype_messaging.notify_devs', ['Executed staff_working_time.dst_time_update(%s)' % subcon_ids]) 			
    conn.close()		
    	
				
def run_tests():
    """
    >>> dst_time_update([3000, 3046, 3049])
    """
	

if __name__ == '__main__':
    import doctest
    doctest.testmod()