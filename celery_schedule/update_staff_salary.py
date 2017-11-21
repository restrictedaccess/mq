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
    
    logging.info('Executing update_staff_salary.process(%s)' % subcon_id)
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    c = db.cursor()
    
    #get the schedule setting	
    sql = "SELECT id, scheduled_date, rate, work_status, status, added_by_id, added_by_type FROM subcontractors_scheduled_subcon_rate WHERE status='waiting' AND subcontractors_id=%s" % subcon_id    
    c.execute(sql)
    result = dictfetchall(c)
    if result:
        scheduled_rate = result[0]
        staff_salary = scheduled_rate['rate']	
        
        
        #Get the current staff monthly and hourly rate
        sql = "SELECT leads_id, userid, php_monthly, php_hourly, work_status FROM subcontractors WHERE id=%s" % subcon_id
        c.execute(sql)
        current_rate = dictfetchall(c)
        current_rate = current_rate[0]
        
        #pending new hourly rate		
        if scheduled_rate['work_status'] == 'Part-Time' :	
            staff_salary_hourly = ((((float(staff_salary) * 12 ) / 52 ) / 5 ) / 4 )		
        else :
            staff_salary_hourly = ((((float(staff_salary) * 12 ) / 52 ) / 5 ) / 8 )

        
        staff_salary_hourly = '%0.2f' % staff_salary_hourly
        current_staff_salary_hourly	= '%0.2f' % current_rate['php_hourly']
        staff_salary = '%0.2f' % staff_salary
        
        str=""
        
        if ('%s' % current_rate['php_monthly']) != ('%s' % staff_salary):
            str += 'STAFF MONTHLY SALARY from %s to %s<br>' % (current_rate['php_monthly'], staff_salary)	
            
        if ('%s' % current_staff_salary_hourly) != ('%s' % staff_salary_hourly):        	
            str += 'STAFF HOURLY SALARY from %s to %s<br>' % (current_staff_salary_hourly, staff_salary_hourly)
    
        
        db_update = MySQLdb.connect(**settings.DB_ARGS)
        cursor = db_update.cursor()
        
        now = get_ph_time()
        client_price_effective_date = date(now.year, now.month, now.day)
        
        #update subcontractors table
        sql = "UPDATE subcontractors SET php_monthly=%s , php_hourly='%s' WHERE id=%s" % (staff_salary, staff_salary_hourly, subcon_id)
        cursor.execute(sql)
        
        cursor.execute("set autocommit = 1")
        
        #mag add ng history sa subcontractors_history
        changes = 'SYSTEM EXECUTED SCHEDULED STAFF CONTRACT SALARY UPDATES.<br>%s' % str
        sql = "INSERT INTO subcontractors_history (subcontractors_id, date_change, changes, change_by_id, change_by_type, changes_status, note) VALUES (%s, '%s', '%s', %s, '%s','%s', '%s')" % (subcon_id, now, changes, 5, 'admin', 'approved', 'Executed via celery')
        cursor.execute(sql)
        history_id = cursor.lastrowid
        
        
        #insert new record sa subcontractors_staff_rate
        sql= "INSERT INTO subcontractors_staff_rate (subcontractors_id, start_date, rate, work_status) VALUES (%s, '%s', %s, '%s')" % (subcon_id, scheduled_rate['scheduled_date'], staff_salary, scheduled_rate['work_status'])
        cursor.execute(sql)
        subcontractors_staff_rate_id = cursor.lastrowid
        
        
        
        if settings.DEBUG:
            mongo_client = MongoClient(host=settings.MONGO_TEST)
        else:
            mongo_client = MongoClient(host=settings.MONGO_PROD , port=27017)
            
        mongodb = mongo_client.subcontractors
        col = mongodb.update_rates_comments
        doc = col.find_one(  {"subcontractors_id" : int(subcon_id),  "subcontractors_scheduled_subcon_rate_id" : int(scheduled_rate['id']) }  )    
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
            sql = "UPDATE subcontractors_history SET note='%s' WHERE id=%s" % (doc["comment_note"], history_id)
            cursor.execute(sql)
    

        #need i-update yung subcontractors_scheduled_subcon_rate
        sql = "UPDATE subcontractors_scheduled_subcon_rate SET status = 'executed' WHERE status='waiting' AND subcontractors_id=%s" % subcon_id
        cursor.execute(sql)	
    
    
        #Send email		
		#set up the recipients
        recipients=['admin@remotestaff.com.au']
        
        #admin who scheduled the update  => s.added_by_id
        sql = "SELECT admin_email FROM admin WHERE admin_id=%s" % scheduled_rate['added_by_id']
        c.execute(sql)
        admin = dictfetchall(c)
        admin_email = admin[0]['admin_email']        
        recipients.append('%s' % admin_email)  
        
        
        #client csro
        sql = "SELECT csro_id FROM leads WHERE id=%s" % current_rate['leads_id']
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
        html_message = "<p>Executed scheduled <strong>client price</strong> updates for contract #%s<br><small>via celery</small></p>" % subcon_id
        to=recipients		
        cc=[]        		
        bcc=['devs@remotestaff.com.au']
        
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = get_ph_time(True),
            generated_by = 'celery update_staff_salary.process',
            html = html_message,
            subject = 'Staff Salary Updated for Contract #%s' % subcon_id,
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)
    
    
        #close mysql connections
        db_update.commit()    
        cursor.close()
        logging.info('Finish executing update_staff_salary.process(%s)' % subcon_id)
    else:
        logging.info('Failed executing update_staff_salary.process(%s)' % subcon_id)
    
def run_tests():
    """
    >>> process(3046)
    """
	

if __name__ == '__main__':
    import doctest
    doctest.testmod()    
     