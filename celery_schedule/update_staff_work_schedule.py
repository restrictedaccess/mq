#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level

from celery.task import task
from celery.execute import send_task
import settings
import logging

import MySQLdb

from pymongo import MongoClient
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal
import couchdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
couch_server = couchdb.Server(settings.COUCH_DSN)
couch_mailbox = couch_server['mailbox']

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
    
@task(ignore_result=True)
def process(subcon_id):
    
    logging.info('Executing celery task update_staff_work_schedule.process(%s)' % subcon_id)
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    c = db.cursor()
    
    sql = "SELECT id, admin_id, working_hours, working_days, work_days, work_status, mon_start, mon_finish, mon_number_hrs, mon_start_lunch, mon_finish_lunch, mon_lunch_number_hrs, tue_start, tue_finish, tue_number_hrs, tue_start_lunch, tue_finish_lunch, tue_lunch_number_hrs, wed_start, wed_finish, wed_number_hrs, wed_start_lunch, wed_finish_lunch, wed_lunch_number_hrs, thu_start, thu_finish, thu_number_hrs, thu_start_lunch, thu_finish_lunch, thu_lunch_number_hrs, fri_start, fri_finish, fri_number_hrs, fri_start_lunch, fri_finish_lunch, fri_lunch_number_hrs, sat_start, sat_finish, sat_number_hrs, sat_start_lunch, sat_finish_lunch, sat_lunch_number_hrs, sun_start, sun_finish, sun_number_hrs, sun_start_lunch, sun_finish_lunch, sun_lunch_number_hrs FROM subcontractors_scheduled_working_time WHERE status='waiting' AND subcon_id=%s" % subcon_id
    c.execute(sql)
    result = dictfetchall(c)
    if result:
        scheduled = result[0]
        
        str=""
        if scheduled:
            sql="SELECT leads_id, working_hours, working_days, work_days, work_status,mon_start, mon_finish, mon_number_hrs, mon_start_lunch, mon_finish_lunch, mon_lunch_number_hrs, tue_start, tue_finish, tue_number_hrs, tue_start_lunch, tue_finish_lunch, tue_lunch_number_hrs, wed_start, wed_finish, wed_number_hrs, wed_start_lunch, wed_finish_lunch, wed_lunch_number_hrs, thu_start, thu_finish, thu_number_hrs, thu_start_lunch, thu_finish_lunch, thu_lunch_number_hrs, fri_start, fri_finish, fri_number_hrs, fri_start_lunch, fri_finish_lunch, fri_lunch_number_hrs, sat_start, sat_finish, sat_number_hrs, sat_start_lunch, sat_finish_lunch, sat_lunch_number_hrs, sun_start, sun_finish, sun_number_hrs, sun_start_lunch, sun_finish_lunch, sun_lunch_number_hrs FROM subcontractors WHERE id=%s" % subcon_id
            c.execute(sql)
            result = dictfetchall(c)
            subcon = result[0]
            
            #for key, value in dict.items(subcon):             
                #if '%s' % subcon[key] != '%s' % scheduled[key]:
                #    str += "%s from %s to %s<br>" % ('%s' % key, '%s' % subcon[key], '%s' % subcon[key])
                
                
            if '%s' % subcon["working_hours"] != '%s' % scheduled["working_hours"]:
                str += "Working Hours from %s to %s<br>" % ('%s' % subcon["working_hours"], '%s' % scheduled["working_hours"])    
                
            if '%s' % subcon["working_days"] != '%s' % scheduled["working_days"]:
                str += 'No. Work Days from %s to %s<br>' % ('%s' % subcon["working_days"], '%s' % scheduled["working_days"])

            if '%s' % subcon["work_days"] != '%s' % scheduled["work_days"]:
                str += 'Working Days from %s to %s<br>' % ('%s' % subcon["work_days"], '%s' % scheduled["work_days"])
            
            if '%s' % subcon["work_status"] != '%s' % scheduled["work_status"]:
                str += 'Working Status from %s to %s<br>' % ('%s' % subcon["work_status"], '%s' % scheduled["work_status"])     
                
            
            #Monday Schedule  
            if '%s' % subcon["mon_start"] != '%s' % scheduled["mon_start"]:
                str += 'Monday Start Work from %s to %s<br>' % ('%s' % subcon["mon_start"], '%s' % scheduled["mon_start"])

            if '%s' % subcon["mon_finish"] != '%s' % scheduled["mon_finish"]:
                str += 'Monday Finish Work from %s to %s<br>' % ('%s' % subcon["mon_finish"], '%s' % scheduled["mon_finish"])        
        
            if '%s' % subcon["mon_number_hrs"] != '%s' % scheduled["mon_number_hrs"]:
                str += 'Monday Work Hours from %s to %s<br>' % ('%s' % subcon["mon_number_hrs"], '%s' % scheduled["mon_number_hrs"])
            
            if '%s' % subcon["mon_start_lunch"] != '%s' % scheduled["mon_start_lunch"]:
                str += 'Monday Start Lunch from %s to %s<br>' % ('%s' % subcon["mon_start_lunch"], '%s' % scheduled["mon_start_lunch"])
        
            if '%s' % subcon["mon_finish_lunch"] != '%s' % scheduled["mon_finish_lunch"]:
                str += 'Monday Finish Lunch from %s to %s<br>' % ('%s' % subcon["mon_finish_lunch"], '%s' % scheduled["mon_finish_lunch"])
        
            if '%s' % subcon["mon_lunch_number_hrs"] != '%s' % scheduled["mon_lunch_number_hrs"]:
                str += 'Monday Lunch Hours from %s to %s<br>' % ('%s' % subcon["mon_lunch_number_hrs"], '%s' % scheduled["mon_lunch_number_hrs"])    
                
                
            #Tuesday Schedule  
            if '%s' % subcon["tue_start"] != '%s' % scheduled["tue_start"]:
                str += 'Tuesday Start Work from %s to %s<br>' % ('%s' % subcon["tue_start"], '%s' % scheduled["tue_start"])

            if '%s' % subcon["tue_finish"] != '%s' % scheduled["tue_finish"]:
                str += 'Tuesday Finish Work from %s to %s<br>' % ('%s' % subcon["tue_finish"], '%s' % scheduled["tue_finish"])        
        
            if '%s' % subcon["tue_number_hrs"] != '%s' % scheduled["tue_number_hrs"]:
                str += 'Tuesday Work Hours from %s to %s<br>' % ('%s' % subcon["tue_number_hrs"], '%s' % scheduled["tue_number_hrs"])
            
            if '%s' % subcon["tue_start_lunch"] != '%s' % scheduled["tue_start_lunch"]:
                str += 'Tuesday Start Lunch from %s to %s<br>' % ('%s' % subcon["tue_start_lunch"], '%s' % scheduled["tue_start_lunch"])
        
            if '%s' % subcon["tue_finish_lunch"] != '%s' % scheduled["tue_finish_lunch"]:
                str += 'Tuesday Finish Lunch from %s to %s<br>' % ('%s' % subcon["tue_finish_lunch"], '%s' % scheduled["tue_finish_lunch"])
        
            if '%s' % subcon["tue_lunch_number_hrs"] != '%s' % scheduled["tue_lunch_number_hrs"]:
                str += 'Tuesday Lunch Hours from %s to %s<br>' % ('%s' % subcon["tue_lunch_number_hrs"], '%s' % scheduled["tue_lunch_number_hrs"])    
                
            #Wednesday Schedule  
            if '%s' % subcon["wed_start"] != '%s' % scheduled["wed_start"]:
                str += 'Wednesday Start Work from %s to %s<br>' % ('%s' % subcon["wed_start"], '%s' % scheduled["wed_start"])

            if '%s' % subcon["wed_finish"] != '%s' % scheduled["wed_finish"]:
                str += 'Wednesday Finish Work from %s to %s<br>' % ('%s' % subcon["wed_finish"], '%s' % scheduled["wed_finish"])        
        
            if '%s' % subcon["wed_number_hrs"] != '%s' % scheduled["wed_number_hrs"]:
                str += 'Wednesday Work Hours from %s to %s<br>' % ('%s' % subcon["wed_number_hrs"], '%s' % scheduled["wed_number_hrs"])
            
            if '%s' % subcon["wed_start_lunch"] != '%s' % scheduled["wed_start_lunch"]:
                str += 'Wednesday Start Lunch from %s to %s<br>' % ('%s' % subcon["wed_start_lunch"], '%s' % scheduled["wed_start_lunch"])
        
            if '%s' % subcon["wed_finish_lunch"] != '%s' % scheduled["wed_finish_lunch"]:
                str += 'Wednesday Finish Lunch from %s to %s<br>' % ('%s' % subcon["wed_finish_lunch"], '%s' % scheduled["wed_finish_lunch"])
        
            if '%s' % subcon["wed_lunch_number_hrs"] != '%s' % scheduled["wed_lunch_number_hrs"]:
                str += 'Wednesday Lunch Hours from %s to %s<br>' % ('%s' % subcon["wed_lunch_number_hrs"], '%s' % scheduled["wed_lunch_number_hrs"]) 


            #Thursday Schedule  
            if '%s' % subcon["thu_start"] != '%s' % scheduled["thu_start"]:
                str += 'Thursday Start Work from %s to %s<br>' % ('%s' % subcon["thu_start"], '%s' % scheduled["thu_start"])

            if '%s' % subcon["thu_finish"] != '%s' % scheduled["thu_finish"]:
                str += 'Thursday Finish Work from %s to %s<br>' % ('%s' % subcon["thu_finish"], '%s' % scheduled["thu_finish"])        
        
            if '%s' % subcon["thu_number_hrs"] != '%s' % scheduled["thu_number_hrs"]:
                str += 'Thursday Work Hours from %s to %s<br>' % ('%s' % subcon["thu_number_hrs"], '%s' % scheduled["thu_number_hrs"])
            
            if '%s' % subcon["thu_start_lunch"] != '%s' % scheduled["thu_start_lunch"]:
                str += 'Thursday Start Lunch from %s to %s<br>' % ('%s' % subcon["thu_start_lunch"], '%s' % scheduled["thu_start_lunch"])
        
            if '%s' % subcon["thu_finish_lunch"] != '%s' % scheduled["thu_finish_lunch"]:
                str += 'Thursday Finish Lunch from %s to %s<br>' % ('%s' % subcon["thu_finish_lunch"], '%s' % scheduled["thu_finish_lunch"])
        
            if '%s' % subcon["thu_lunch_number_hrs"] != '%s' % scheduled["thu_lunch_number_hrs"]:
                str += 'Thursday Lunch Hours from %s to %s<br>' % ('%s' % subcon["thu_lunch_number_hrs"], '%s' % scheduled["thu_lunch_number_hrs"]) 


            #Friday Schedule  
            if '%s' % subcon["fri_start"] != '%s' % scheduled["fri_start"]:
                str += 'Friday Start Work from %s to %s<br>' % ('%s' % subcon["fri_start"], '%s' % scheduled["fri_start"])

            if '%s' % subcon["fri_finish"] != '%s' % scheduled["fri_finish"]:
                str += 'Friday Finish Work from %s to %s<br>' % ('%s' % subcon["fri_finish"], '%s' % scheduled["fri_finish"])        
        
            if '%s' % subcon["fri_number_hrs"] != '%s' % scheduled["fri_number_hrs"]:
                str += 'Friday Work Hours from %s to %s<br>' % ('%s' % subcon["fri_number_hrs"], '%s' % scheduled["fri_number_hrs"])
            
            if '%s' % subcon["fri_start_lunch"] != '%s' % scheduled["fri_start_lunch"]:
                str += 'Friday Start Lunch from %s to %s<br>' % ('%s' % subcon["fri_start_lunch"], '%s' % scheduled["fri_start_lunch"])
        
            if '%s' % subcon["fri_finish_lunch"] != '%s' % scheduled["fri_finish_lunch"]:
                str += 'Friday Finish Lunch from %s to %s<br>' % ('%s' % subcon["fri_finish_lunch"], '%s' % scheduled["fri_finish_lunch"])
        
            if '%s' % subcon["fri_lunch_number_hrs"] != '%s' % scheduled["fri_lunch_number_hrs"]:
                str += 'Friday Lunch Hours from %s to %s<br>' % ('%s' % subcon["fri_lunch_number_hrs"], '%s' % scheduled["fri_lunch_number_hrs"]) 


            #Saturday Schedule  
            if '%s' % subcon["sat_start"] != '%s' % scheduled["sat_start"]:
                str += 'Saturday Start Work from %s to %s<br>' % ('%s' % subcon["sat_start"], '%s' % scheduled["sat_start"])

            if '%s' % subcon["sat_finish"] != '%s' % scheduled["sat_finish"]:
                str += 'Saturday Finish Work from %s to %s<br>' % ('%s' % subcon["sat_finish"], '%s' % scheduled["sat_finish"])        
        
            if '%s' % subcon["sat_number_hrs"] != '%s' % scheduled["sat_number_hrs"]:
                str += 'Saturday Work Hours from %s to %s<br>' % ('%s' % subcon["sat_number_hrs"], '%s' % scheduled["sat_number_hrs"])
            
            if '%s' % subcon["sat_start_lunch"] != '%s' % scheduled["sat_start_lunch"]:
                str += 'Saturday Start Lunch from %s to %s<br>' % ('%s' % subcon["sat_start_lunch"], '%s' % scheduled["sat_start_lunch"])
        
            if '%s' % subcon["sat_finish_lunch"] != '%s' % scheduled["sat_finish_lunch"]:
                str += 'Saturday Finish Lunch from %s to %s<br>' % ('%s' % subcon["sat_finish_lunch"], '%s' % scheduled["sat_finish_lunch"])
        
            if '%s' % subcon["sat_lunch_number_hrs"] != '%s' % scheduled["sat_lunch_number_hrs"]:
                str += 'Saturday Lunch Hours from %s to %s<br>' % ('%s' % subcon["sat_lunch_number_hrs"], '%s' % scheduled["sat_lunch_number_hrs"])


            #Sunday Schedule  
            if '%s' % subcon["sun_start"] != '%s' % scheduled["sun_start"]:
                str += 'Sunday Start Work from %s to %s<br>' % ('%s' % subcon["sun_start"], '%s' % scheduled["sun_start"])

            if '%s' % subcon["sun_finish"] != '%s' % scheduled["sun_finish"]:
                str += 'Sunday Finish Work from %s to %s<br>' % ('%s' % subcon["sun_finish"], '%s' % scheduled["sun_finish"])        
        
            if '%s' % subcon["sun_number_hrs"] != '%s' % scheduled["sun_number_hrs"]:
                str += 'Sunday Work Hours from %s to %s<br>' % ('%s' % subcon["sun_number_hrs"], '%s' % scheduled["sun_number_hrs"])
            
            if '%s' % subcon["sun_start_lunch"] != '%s' % scheduled["sun_start_lunch"]:
                str += 'Sunday Start Lunch from %s to %s<br>' % ('%s' % subcon["sun_start_lunch"], '%s' % scheduled["sun_start_lunch"])
        
            if '%s' % subcon["sun_finish_lunch"] != '%s' % scheduled["sun_finish_lunch"]:
                str += 'Sunday Finish Lunch from %s to %s<br>' % ('%s' % subcon["sun_finish_lunch"], '%s' % scheduled["sun_finish_lunch"])
        
            if '%s' % subcon["sun_lunch_number_hrs"] != '%s' % scheduled["sun_lunch_number_hrs"]:
                str += 'Sunday Lunch Hours from %s to %s<br>' % ('%s' % subcon["sun_lunch_number_hrs"], '%s' % scheduled["sun_lunch_number_hrs"])         
                
            
            #null values excemption
            
            #moday
            if scheduled["mon_start"]:
                mon_start = "'%s'" % scheduled["mon_start"]
            else:    
                mon_start = "NULL"
                
            if scheduled["mon_finish"]:
                mon_finish = "'%s'" % scheduled["mon_finish"]
            else:    
                mon_finish = "NULL"
                
            if scheduled["mon_start_lunch"]:
                mon_start_lunch = "'%s'" % scheduled["mon_start_lunch"]
            else:    
                mon_start_lunch = "NULL"
                
            if scheduled["mon_finish_lunch"]:
                mon_finish_lunch = "'%s'" % scheduled["mon_finish_lunch"]
            else:    
                mon_finish_lunch = "NULL"

            #tuesday
            if scheduled["tue_start"]:
                tue_start = "'%s'" % scheduled["tue_start"]
            else:    
                tue_start = "NULL"
                
            if scheduled["tue_finish"]:
                tue_finish = "'%s'" % scheduled["tue_finish"]
            else:    
                tue_finish = "NULL"
                
            if scheduled["tue_start_lunch"]:
                tue_start_lunch = "'%s'" % scheduled["tue_start_lunch"]
            else:    
                tue_start_lunch = "NULL"
                
            if scheduled["tue_finish_lunch"]:
                tue_finish_lunch = "'%s'" % scheduled["tue_finish_lunch"]
            else:    
                tue_finish_lunch = "NULL"   
                
            #wednesday
            if scheduled["wed_start"]:
                wed_start = "'%s'" % scheduled["wed_start"]
            else:    
                wed_start = "NULL"
                
            if scheduled["wed_finish"]:
                wed_finish = "'%s'" % scheduled["wed_finish"]
            else:    
                wed_finish = "NULL"
                
            if scheduled["wed_start_lunch"]:
                wed_start_lunch = "'%s'" % scheduled["wed_start_lunch"]
            else:    
                wed_start_lunch = "NULL"
                
            if scheduled["wed_finish_lunch"]:
                wed_finish_lunch = "'%s'" % scheduled["wed_finish_lunch"]
            else:    
                wed_finish_lunch = "NULL"
                
                
            #thursday
            if scheduled["thu_start"]:
                thu_start = "'%s'" % scheduled["thu_start"]
            else:    
                thu_start = "NULL"
                
            if scheduled["thu_finish"]:
                thu_finish = "'%s'" % scheduled["thu_finish"]
            else:    
                thu_finish = "NULL"
                
            if scheduled["thu_start_lunch"]:
                thu_start_lunch = "'%s'" % scheduled["thu_start_lunch"]
            else:    
                thu_start_lunch = "NULL"
                
            if scheduled["thu_finish_lunch"]:
                thu_finish_lunch = "'%s'" % scheduled["thu_finish_lunch"]
            else:    
                thu_finish_lunch = "NULL"      

            #friday        
            if scheduled["fri_start"]:
                fri_start = "'%s'" % scheduled["fri_start"]
            else:    
                fri_start = "NULL"
                
            if scheduled["fri_finish"]:
                fri_finish = "'%s'" % scheduled["fri_finish"]
            else:    
                fri_finish = "NULL"
                
            if scheduled["fri_start_lunch"]:
                fri_start_lunch = "'%s'" % scheduled["fri_start_lunch"]
            else:    
                fri_start_lunch = "NULL"
                
            if scheduled["fri_finish_lunch"]:
                fri_finish_lunch = "'%s'" % scheduled["fri_finish_lunch"]
            else:    
                fri_finish_lunch = "NULL"


            #saturday
            if scheduled["sat_start"]:
                sat_start = "'%s'" % scheduled["sat_start"]
            else:    
                sat_start = "NULL"
                
            if scheduled["sat_finish"]:
                sat_finish = "'%s'" % scheduled["sat_finish"]
            else:    
                sat_finish = "NULL"
                
            if scheduled["sat_start_lunch"]:
                sat_start_lunch = "'%s'" % scheduled["sat_start_lunch"]
            else:    
                sat_start_lunch = "NULL"
                
            if scheduled["sat_finish_lunch"]:
                sat_finish_lunch = "'%s'" % scheduled["sat_finish_lunch"]
            else:    
                sat_finish_lunch = "NULL"

            #sunday
            if scheduled["sun_start"]:
                sun_start = "'%s'" % scheduled["sun_start"]
            else:    
                sun_start = "NULL"
                
            if scheduled["sun_finish"]:
                sun_finish = "'%s'" % scheduled["sun_finish"]
            else:    
                sun_finish = "NULL"
                
            if scheduled["sun_start_lunch"]:
                sun_start_lunch = "'%s'" % scheduled["sun_start_lunch"]
            else:    
                sun_start_lunch = "NULL"
                
            if scheduled["sun_finish_lunch"]:
                sun_finish_lunch = "'%s'" % scheduled["sun_finish_lunch"]
            else:    
                sun_finish_lunch = "NULL"            
                
            db_update = MySQLdb.connect(**settings.DB_ARGS)
            cursor = db_update.cursor()
            
            #update subcontractors table
            sql = "UPDATE subcontractors SET working_hours='%s', working_days=%s, work_days='%s', work_status='%s', " % (scheduled["working_hours"], scheduled["working_days"], scheduled["work_days"], scheduled["work_status"]) 
            sql += "mon_start=%s, mon_finish=%s, mon_number_hrs=%s, " % (mon_start, mon_finish, scheduled["mon_number_hrs"])
            sql += "mon_start_lunch=%s, mon_finish_lunch=%s, mon_lunch_number_hrs=%s, " % (mon_start_lunch, mon_finish_lunch, scheduled["mon_lunch_number_hrs"])
            sql += "tue_start=%s, tue_finish=%s, tue_number_hrs=%s, " % (tue_start, tue_finish, scheduled["tue_number_hrs"])
            sql += "tue_start_lunch=%s, tue_finish_lunch=%s, tue_lunch_number_hrs=%s, " % (tue_start_lunch, tue_finish_lunch, scheduled["tue_lunch_number_hrs"])
            sql += "wed_start=%s, wed_finish=%s, wed_number_hrs=%s, " % (wed_start, wed_finish, scheduled["wed_number_hrs"])
            sql += "wed_start_lunch=%s, wed_finish_lunch=%s, wed_lunch_number_hrs=%s, " % (wed_start_lunch, wed_finish_lunch, scheduled["wed_lunch_number_hrs"])
            sql += "thu_start=%s, thu_finish=%s, thu_number_hrs=%s, " % (thu_start, thu_finish, scheduled["thu_number_hrs"])
            sql += "thu_start_lunch=%s, thu_finish_lunch=%s, thu_lunch_number_hrs=%s, " % (thu_start_lunch, thu_finish_lunch, scheduled["thu_lunch_number_hrs"])
            sql += "fri_start=%s, fri_finish=%s, fri_number_hrs=%s, " % (fri_start, fri_finish, scheduled["fri_number_hrs"])
            sql += "fri_start_lunch=%s, fri_finish_lunch=%s, fri_lunch_number_hrs=%s, " % (fri_start_lunch, fri_finish_lunch, scheduled["fri_lunch_number_hrs"])
            sql += "sat_start=%s, sat_finish=%s, sat_number_hrs=%s, " % (sat_start, sat_finish, scheduled["sat_number_hrs"])
            sql += "sat_start_lunch=%s, sat_finish_lunch=%s, sat_lunch_number_hrs=%s, " % (sat_start_lunch, sat_finish_lunch, scheduled["sat_lunch_number_hrs"])
            sql += "sun_start=%s, sun_finish=%s, sun_number_hrs=%s, " % (sun_start, sun_finish, scheduled["sun_number_hrs"])
            sql += "sun_start_lunch=%s, sun_finish_lunch=%s, sun_lunch_number_hrs=%s " % (sun_start_lunch, sun_finish_lunch, scheduled["sun_lunch_number_hrs"])
            sql += "WHERE id =%s" % subcon_id
            
            cursor.execute(sql)
            
            cursor.execute("set autocommit = 1")
            
            #mag add ng history sa subcontractors_history
            changes = 'SYSTEM EXECUTED SCHEDULED WORKING TIME UPDATES.<br><b>Changes made : </b><br>%s' % str
            sql = "INSERT INTO subcontractors_history (subcontractors_id, date_change, changes, change_by_id, change_by_type, changes_status, note) VALUES (%s, '%s', '%s', %s, '%s','%s', '%s')" % (subcon_id, get_ph_time(), changes, 5, 'admin', 'approved', 'Executed via celery')
            cursor.execute(sql)
            
            
            #need i-update yung subcontractors_scheduled_working_time
            sql = "UPDATE subcontractors_scheduled_working_time SET status = 'executed', date_executed='%s' WHERE id=%s;" % (get_ph_time(), scheduled["id"])
            cursor.execute(sql)
            
            #Send email		
            #set up the recipients
            recipients=['admin@remotestaff.com.au']
            
            
            #admin who scheduled the update 
            sql = "SELECT admin_email FROM admin WHERE admin_id=%s" % scheduled['admin_id']
            c.execute(sql)
            admin = dictfetchall(c)
            admin_email = admin[0]['admin_email']        
            recipients.append('%s' % admin_email)
            
            
            #client csro
            sql = "SELECT csro_id FROM leads WHERE id=%s" % subcon['leads_id']
            c.execute(sql)
            if c:
                csro = dictfetchall(c)
                csro_id = csro[0]['csro_id']
            
                sql = "SELECT admin_email FROM admin WHERE admin_id=%s" % csro_id
                c.execute(sql)
                csro = dictfetchall(c)
                csro_email = csro[0]['admin_email']
                if csro_email:			
                    recipients.append('%s' % csro_email) 
            
            
            #save email messasge in couchdb mailbox
            html_message = "<p>Executed scheduled <strong>Working Time</strong> updates for contract #%s<br><small>via celery</small></p>%s" % (subcon_id, str)
            to=recipients		
            cc=[]        		
            bcc=['devs@remotestaff.com.au']
        
            mailbox = dict(
                sent = False,		
                bcc = bcc,
                cc = cc,
                created = get_ph_time(True),
                generated_by = 'celery update_staff_work_schedule.process',
                html = html_message,
                subject = 'Staff Working Time Updated for Contract #%s' % subcon_id,
                to = to			
            )
            mailbox['from'] = 'noreply@remotestaff.com.au'		
            couch_mailbox.save(mailbox)
            
            
            #close mysql connections
            db_update.commit()    
            cursor.close()
            
            logging.info('Finish executing update_staff_work_schedule.process(%s)' % subcon_id)
            
    else:
        logging.info('Failed executing update_staff_work_schedule.process(%s)' % subcon_id)
        
        
        
            
        
        
        
def run_tests():
    """
    >>> process(5903)
    """
	

if __name__ == '__main__':
    import doctest
    doctest.testmod()    
     