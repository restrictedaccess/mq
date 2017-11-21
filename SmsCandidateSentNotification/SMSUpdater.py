#!/usr/bin/env python
#   2013-05-20  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -   initial commit
#   -   responsible for SMS related sending
import settings
import MySQLdb
import json
from datetime import datetime

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

class SMSUpdater:
    @staticmethod
    def send_email_alert(subject, msg):
        from email.mime.text import MIMEText
        import smtplib
    
        msg = MIMEText(msg)
        if settings.DEBUG:
            msg['Subject'] = 'TEST %s' % subject
        else:
            msg['Subject'] = subject
    
        msg['From'] = 'remotestaff noreply<noreply@remotestaff.com.au>'
        msg['To'] = settings.EMAIL_ALERT
    
        s = smtplib.SMTP(host= settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )
        s.sendmail('noreply@remotestaff.com.au',
            settings.EMAIL_ALERT, msg.as_string())
        s.quit()

    
    
    @staticmethod
    def updateSMS(message):
        data = message
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("set autocommit = 1") 
        c.execute("UPDATE staff_admin_sms_out_messages SET sent=0 WHERE id=%s", (data["sms_id"]))
        db.commit()
        c.close()
        db.close()
        
    @staticmethod
    def receiveReplySMS(message):
        data = message
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("set autocommit = 1") 
        import urllib2
        url = ""
        #set the list of ping admin
        ping_list = []
        
        
        if data.has_key("sms_id") and data["sms_id"] and data["sms_id"].strip()!="":
            c.execute("SELECT admin_id, userid FROM staff_admin_sms_out_messages WHERE id=%s", (data["sms_id"]))
            result = c.fetchone()
            if result==None:
                SMSUpdater.send_email_alert("Failed to receive reply message: missing sms_id", "Failed to receive reply message because of missing sms_id: {body}".format(body=json.dumps(data)))
                return False
            admin_id, userid = result
            c.execute("INSERT INTO staff_admin_sms_in_messages(message, mobile_number, admin_id, userid, date_created) VALUES(%s, %s, %s, %s, %s)", (data["message"], data["mobile_number"], admin_id, userid, datetime.today()))
        
            c.execute("INSERT INTO sms_admin_notifications(admin_id) VALUES(%s)", (admin_id))
            c.execute("INSERT INTO sms_admin_notification_logs(admin_id, date_created) VALUES(%s, %s)", (admin_id,datetime.today()) )
            
            
            if str(admin_id) not in ping_list:
                ping_list.append(str(admin_id))
            
        
        if data.has_key("original_message") and data.has_key("date_time_received"):
            mobile_number = data["mobile_number"]
            message = data["original_message"]
            date_time_received = data["date_time_received"]
            
            
            if data.has_key("reply_number"):
                reply_number = data["reply_number"]
            else:
                reply_number = mobile_number
            
            #get candidate info
            if settings.DEBUG:
                url = "http://test.remotestaff.com.au/portal/recruiter/get_candidate_info.php?mobile_number="+str(data["mobile_number"])
            else:
                url = "https://remotestaff.com.au/portal/recruiter/get_candidate_info.php?mobile_number="+str(data["mobile_number"])
            
            try:
                candi_info = urllib2.urlopen(url)
                candi_info = json.load(candi_info)
                
                if candi_info["success"]:
                    for candi in candi_info["candidates"]:
                        if str(candi["recruiter_id"]) not in ping_list:
                            ping_list.append(str(candi["recruiter_id"]))
                
            except:
                pass
            
            c.execute("INSERT INTO sms_messages(mobile_number, message, date_received, date_created, mode, reply_number) VALUES(%s, %s, %s, %s, %s, %s)", (mobile_number, message, date_time_received, datetime.today(), "reply", reply_number))
        
            c.execute("SELECT admin_id FROM admin WHERE status = 'COMPLIANCE' OR status = 'FULL-CONTROL' AND status <> 'REMOVED'")
            rows = dictfetchall(c)
            for row in rows:
                admin_id = row["admin_id"]
                admin_id = str(admin_id)
                c.execute("INSERT INTO sms_admin_notifications(admin_id) VALUES(%s)", (admin_id))
                c.execute("INSERT INTO sms_admin_notification_logs(admin_id, date_created) VALUES(%s, %s)", (admin_id,datetime.today()) )
                if str(admin_id) not in ping_list:
                    ping_list.append(str(admin_id))
        
        db.commit()
        
        #finally ping everyone
        for admin_id in ping_list:
            try:
                if settings.DEBUG:
                    url = "http://test.remotestaff.com.au:8000/sms/ping/?admin_id="+str(admin_id)
                else:
                    url = "http://remotestaff.com.au:8000/sms/ping/?admin_id="+str(admin_id)
                urllib2.urlopen(url).read(1000)
            except:
                pass
        c.close()
        db.close()
        
    @staticmethod
    def receiveSendSMS(message):
        data = message
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("set autocommit = 1") 
        
        if data.has_key("original_message") and data.has_key("date_time_received"):
            mobile_number = data["mobile_number"]
            message = data["original_message"]
            date_time_received = data["date_time_received"]
            sms_id = data["sms_id"]
            c.execute("INSERT INTO sms_messages(mobile_number, message, date_sent, date_created, mode, sms_id) VALUES(%s, %s, %s, %s, %s, %s)", (mobile_number, message, date_time_received, datetime.today(), "send", sms_id))
        
        db.commit()
        c.close()
        db.close()