#!/usr/bin/env python
#   2015-09-02  Normaneil E. Macutay <normaneil.macutay@gmail.com>
#   -   Initial commit celery auto suspension of clients staff contracts

from celery.task import task
from celery.execute import send_task
import settings
import logging

import MySQLdb

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
def process(leads_id):
    
    if leads_id:    
        logging.info('Executing suspend.process(%s)' % leads_id)
    
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()

        #get leads info
        sql = "SELECT fname, lname, email FROM leads WHERE id=%s;" % leads_id    
        c.execute(sql)
        result = dictfetchall(c)
        lead = result[0]
        
        #get clients active staff contracts for possible suspension
        sql = "SELECT s.id, s.job_designation, p.userid, p.fname, p.lname, p.email, s.staff_email FROM subcontractors s JOIN personal p ON s.userid = p.userid WHERE s.status='ACTIVE' AND s.leads_id=%s;" % leads_id    
        c.execute(sql)
        subcontractors = dictfetchall(c)
        if subcontractors:
            str=""
            #for subcon in subcontractors:
            #    str += '%s %s \n' % (subcon["id"], subcon["fname"])            
            #return str
            
            
            #new cursor for updating and insertion. As per Allan advice
            db_update = MySQLdb.connect(**settings.DB_ARGS)
            cursor = db_update.cursor()
            now = get_ph_time()
    
            
            #update set status value into "suspended"
            sql = "UPDATE subcontractors SET status='suspended' WHERE status='ACTIVE' AND leads_id=%s;" % leads_id
            cursor.execute(sql)
            
            cursor.execute("set autocommit = 1")
            
            
            #add history
            changes = 'SYSTEM AUTOMATICALLY SUSPENDED STAFF CONTRACT'
            admin_notes = "System suspended staff contract due to client load issue"
            
            for subcon in subcontractors:
                str += "<li>Subcon Id #%s %s %s %s</li>" % (subcon["id"], subcon["fname"], subcon["lname"], subcon["job_designation"])
                sql = "INSERT INTO subcontractors_history (subcontractors_id, date_change, changes, change_by_id, change_by_type, changes_status, note) VALUES (%s, '%s', '%s', %s, '%s','%s', '%s')" % (subcon["id"], now, changes, 5, 'admin', 'suspended', admin_notes)
                cursor.execute(sql)
                
                logging.info(subcon["id"])
                #Send API to add subcon suspension log
                logging.info('Sending API to add subcon suspension log')
                try:
                    api_url = settings.BASE_API_URL+"/timesheet-weeks/add-suspension-logs/";    
                    import pycurl    
                    try:
                        from urllib.parse import urlencode
                    except:
                        from urllib import urlencode
                    
                    curl = pycurl.Curl()
                    curl.setopt(curl.URL, api_url)
                    post_data = { 'subcon_id': subcon["id"], 'date_change' : now, 'status' : 'suspended' }
                    postfields = urlencode(post_data)
                    curl.setopt(curl.POSTFIELDS, postfields)
                    curl.perform()
                    curl.close() 
                except:
                    pass            
            
            #Send email        
            #set up the recipients
            recipients=["devs@remotestaff.com.au"]
            
            #client's email
            #recipients.append('%s' % lead["email"])
            
            #client csro
            sql = "SELECT csro_id FROM leads WHERE id=%s" % leads_id
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
            html_message = "<p><span style='text-transform:capitalize;'>Client #%s %s %s</span> %s active staff" % ( leads_id, lead["fname"], lead["lname"], lead["email"] )
            html_message += " has been temporarily suspended due to client's depleted load balance.</p>"               
            html_message +="<p>Following Staff/s:</p>"
            html_message +="<ol>"
            html_message += str
            html_message +="</ol>"
            html_message +="<p style='color:#999999;'>Remotestaff System<br />System Generated:::celery_suspension.</p>"
            
            to=recipients        
            cc=[]                
            bcc=[]
            
            mailbox = dict(
                sent = False,        
                bcc = bcc,
                cc = cc,
                created = get_ph_time(True),
                generated_by = 'celery suspend.process',
                html = html_message,
                subject = 'Suspension Notice for Client #%s %s %s' % ( leads_id, lead["fname"], lead["lname"] ),
                to = to            
            )
            mailbox['from'] = 'noreply@remotestaff.com.au'        
            couch_mailbox.save(mailbox)
        
            
            #close mysql connections
            db_update.commit()    
            cursor.close()
            
            #api to update client invoice due date
            logging.info('Sending API to update client invoice due date => (%s)' % leads_id)
            try:
                
                api_url = settings.BASE_API_URL+"/xero-contacts/update-due-date/";    
                import pycurl

                try:
                    from urllib.parse import urlencode
                except:
                       from urllib import urlencode
                c = pycurl.Curl()
                c.setopt(c.URL, api_url)
                post_data = { 'leads_id': leads_id }
                postfields = urlencode(post_data)
                c.setopt(c.POSTFIELDS, postfields)
                c.perform()
                c.close() 
            except:
                    pass
            
            
            logging.info('Finished executing suspend.process(%s)' % leads_id)
        else:
            logging.info('Client %s has no active staff ' % leads_id)  
    
def run_tests():
    """
    >>> process(8579)
    """
    

if __name__ == '__main__':
    import doctest
    doctest.testmod()    
     