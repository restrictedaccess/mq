#!/usr/bin/env python
#   2014-07-04  Normaneil E. Macutay <normanm@remotestaff.com.au>
#   -   initial commit for activating client suspended staff contracts


from sqlalchemy.sql import text
from persistent_mysql_connection import engine
from celery.task import task, Task
from celery.execute import send_task
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
import couchdb
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
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
def activate(leads_id):
    logging.info('activating client[%s] staffs' % leads_id)
	
    conn = engine.connect()	
	
    sql = text("""SELECT fname, lname, email, csro_id FROM leads WHERE id=:leads_id;""")	
    client = conn.execute(sql, leads_id=leads_id).fetchone()
	
    admin_email = 'accounts@remotestaff.com.au'	
    if client.csro_id:
        sql = text("""SELECT admin_email FROM admin WHERE admin_id=:csro_id;""")	
        admin = conn.execute(sql, csro_id=client.csro_id).fetchone() 	
        admin_email = admin.admin_email	
	
    sql = text("""SELECT s.id , s.staff_email, s.job_designation, p.fname, p.lname FROM subcontractors s JOIN personal p ON p.userid = s.userid WHERE s.status='suspended' AND s.leads_id=:leads_id;""")	
    staffs = conn.execute(sql, leads_id=leads_id).fetchall()   
    
    
    err_msg=""
    str=""	
    if not staffs:
        err_msg = 'Client %s has no existing suspended staff contracts ' % leads_id	
        send_task('skype_messaging.notify_devs', [ err_msg ])
        		
    str=""
    now = get_ph_time()	
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]	
    for s in staffs:
        str += '<li>#%s %s %s %s</li>' % (int(s.id), s.fname, s.lname, s.job_designation )
        subcon_id = int(s.id)
		
        #update subcontractors table
        sql = text("""UPDATE subcontractors SET status='ACTIVE' WHERE id = :subcon_id ;""")
        conn.execute(sql, subcon_id = subcon_id)

        #mag add ng history sa subcontractors_history
        sql = text("""
            INSERT INTO subcontractors_history
            (subcontractors_id, date_change, changes, change_by_id, change_by_type, changes_status, note)
            VALUES (:subcontractors_id, :date_change, :changes, :change_by_id, :change_by_type, :changes_status, :note)
        """)
        conn.execute(sql,
            subcontractors_id = subcon_id,
            date_change = now,
            changes = 'Activated suspended staff contract. Status set to ACTIVE.',
            change_by_id = 5,
            change_by_type = 'admin', 			
            changes_status = 'updated',
            note = 'Automactically activated.'		
        )		
		
        #send email to subcon
        to=['%s' % s.staff_email]		
        cc=None 		
        bcc=None
        
        html_message = '<div style="font-family: lucida grande,tahoma,verdana,arial,sans-serif; font-size: 11px;"><div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div><p style="text-transform:capitalize;">Dear %s %s,</p><p>Your contract status to Client %s has been <strong>ACTIVATED</strong> and set your contract status to "ACTIVE".</p><p>You can now continue working with your client as %s.</p><P>admin@remotestaff.com.au</P></div>' % (s.fname, s.lname, client.fname, s.job_designation)
		
        mailbox = dict(
            sent = False,		
            bcc = bcc,
            cc = cc,
            created = date_created,
            generated_by = 'celery activate_client_suspended_staff_contracts.activate',
            html = html_message,
            subject = 'Staff %s %s Contract Activated' % (s.fname, s.lname),
            to = to			
        )
        mailbox['from'] = 'noreply@remotestaff.com.au'		
        couch_mailbox.save(mailbox)		
        		
    #send email to CSRO
    to=['%s' % admin_email]		
    cc=None 		
    bcc=['devs@remotestaff.com.au']
        
    html_message = '<div style="font-family: lucida grande,tahoma,verdana,arial,sans-serif; font-size: 11px;"><div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div><p style="text-transform:capitalize;">Dear Admin ,</p><p>Client #%s %s %s following staffs contract has been <strong>ACTIVATED</strong> and set contract status to  "ACTIVE".</p><ol>%s</ol><p>&nbsp;</p><P style="color:#999999;font-family:Courier New, Courier, monospace;">System Generated</P></div>' % (leads_id, client.fname, client.lname, str)
		
    mailbox = dict(
        sent = False,		
        bcc = bcc,
        cc = cc,
        created = date_created,
        generated_by = 'celery activate_client_suspended_staff_contracts.activate',
        html = html_message,
        subject = 'STAFFS CONTRACT ACTIVATED ALERT',
        to = to			
    )
    mailbox['from'] = 'noreply@remotestaff.com.au'		
    couch_mailbox.save(mailbox)			
    conn.close()			
    logging.info('activated client[%s] staffs' % leads_id)    

def run_tests():
    """
    >>> activate(8984)
    """
	

if __name__ == '__main__':
    import doctest
    doctest.testmod()
