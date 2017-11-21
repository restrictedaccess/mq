#   2014-04-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add exception, used rackspace smtp if ses connection failes
#   2013-08-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated default mail recepient on test
#   2013-08-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   support for unicode text
#   2013-06-17  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added In-Reply-To and References email headers in preparation for email to ticket
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added rate_limit on sending email
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-28  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered sender and reply_to field
#   2013-03-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import couchdb
import re
import string
import sys
from pprint import pformat

from sqlalchemy.sql import text
import pika
from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

utc = timezone('UTC')
phtz = timezone('Asia/Manila')


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
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)


@task(ignore_result=True, rate_limit='14/s')
def send(doc_id):
    logging.info('Received %s' % doc_id)
    """given the doc_id, send email
    """
    #couchdb settings
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db = couch_server['mailbox']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    msg = MIMEMultipart()

    #text part
    if doc.has_key('text'):
        if doc['text'] != None :
            text_message = string.strip(doc['text'])
            if text_message != '':
                part1 = MIMEText('%s' % text_message, 'plain', 'utf-8')
                part1.add_header('Content-Disposition', 'inline')
                msg.attach(part1)

    if doc.has_key('html'):
        if doc['html'] != None :
            html_message = string.strip(doc['html'])
            if html_message != '':
                part2 = MIMEText('%s' % html_message, 'html', 'UTF-8')
                part2.add_header('Content-Disposition', 'inline')
                msg.attach(part2)

    subject = doc['subject']
    if settings.DEBUG == True:
        #subject = 'TEST %s' % subject
        subject = 'TEST Recipients : %s . %s' % (string.join(doc['to'], ','), subject)
        
    msg['Subject'] = subject

    if doc.has_key('from'):
        msg['From'] = doc['from']
    else:
        msg['From'] = "noreply@remotestaff.com.au";
            
    try:
       msg['To'] = string.join(doc['to'], ',')
    except:
       msg['To'] = 'devs@remotestaff.com.au'
    try:
        if doc.has_key('cc'):
            if doc['cc'] != None:
                msg['Cc'] = string.join(doc['cc'], ',')
    except:
        pass
    #attachments
    try:
        if doc.has_key('_attachments'):
            for attachment in doc['_attachments']:
                f = db.get_attachment(doc, attachment)
                data = f.read()
                file_attachment = MIMEApplication(data)
                file_attachment.add_header('Content-Disposition', 'attachment', filename=attachment)
                msg.attach(file_attachment)
    except:
        pass    

    

    #collect recipients
    recipients = []
    if settings.DEBUG:
        recipients.append('devs@remotestaff.com.au')
    else:
        for email in doc['to']:
            recipients.append(email)
        try:
            if doc.has_key('cc'):
                if doc['cc'] != None:
                    for email in doc['cc']:
                        recipients.append(email)
        except:
            pass
        if doc.has_key('bcc'):
            if doc['bcc'] != None:
                for email in doc['bcc']:
                    recipients.append(email)

    #add reply_to field
    reply_to = doc.get('reply_to')
    if reply_to not in (None, ''):
        msg['Reply-To'] = reply_to

    sender = doc.get('sender')
    if sender not in (None, ''):
        msg['Sender'] = sender

    #add In-Reply-To and References headers
    in_reply_to = doc.get('In-Reply-To')
    if in_reply_to != None:
        msg['In-Reply-To'] = in_reply_to

    references = doc.get('References')
    if references != None:
        msg['References'] = references

    
    
    mailer = ""
    
    is_invoice = False
    
    if doc.has_key("is_invoice"):
        if subject == None or subject == "":
            subject = "Remotestaff Tax Invoice # %s" % doc["order_id"]
    
    #Injected By Josef Balisalisa START
     
    import sendgrid
    import os
    from sendgrid.helpers.mail import *
    from pymongo import MongoClient
    import re
    import base64
    import pprint
     
    is_invoice = False
    logging.info("Evaluating if email is invoice")
    try:
        if doc.has_key('is_invoice'):
         
            logging.info("Sending as Invoice via Send Grid %s " % doc_id)
             
             
            mailer = "Send Grid"
             
            sg = sendgrid.SendGridAPIClient(apikey=settings.SENDGRID_API_KEY)

            debugging_env = "prod"
            if settings.DEBUG:
                debugging_env = "development"
             
            data = {
                "personalizations": [
                    {
                      "to": [
                                 
                             ],
                      "subject": subject
                    }
                ],
                "from": {
                         "email": doc["from"]
                },
                "content": [
                             
                ],
                "custom_args": {
                    "accounts_order_id": doc["order_id"],
                    "debugging_env": debugging_env,
                    "couch_id": doc_id
                },

            }
             
             
             
             
            data_to_save = {}
             
            data_to_save["from"] = doc["from"]
             
            if doc.has_key('html'):
                if doc['html'] != None :
                    html_message = string.strip(doc['html'])
                    data["content"].append({"type":"text/html", "value": html_message})
                    data_to_save["html_content"] = html_message
                     
            if doc.has_key('text'):
                if doc['text'] != None :
                    text_message = string.strip(doc['text'])
                    data["content"].append({"type":"text/plain", "value": text_message})
                    data_to_save["text_content"] = text_message
                     
            data_to_save["to"] = []
            data_to_save["cc"] = []
            data_to_save["bcc"] = []
             
            if settings.DEBUG:

#                 data["personalizations"][0]["to"].append({"email":"devs@remotestaff.com.au"})
#                 data_to_save["to"].append("devs@remotestaff.com.au")
                logging.info("SendGrid TEST")

                # data["mail_settings"] = {}
                # data["mail_settings"]["sandbox_mode"] = {}
                # data["mail_settings"]["sandbox_mode"]["enable"]  = True

                # logging.info("SendGrid Sandbox details: %s " % data["mail_settings"])

                
                emails_appended = []
                
                for email in recipients:
                    if email.lower() not in emails_appended:
                        data["personalizations"][0]["to"].append({"email":email})
                        emails_appended.append(email.lower())
                        data_to_save["to"].append(email)
                        
                logging.info("sending to %s " % data["personalizations"][0]["to"])

            else:
                logging.info("SendGrid Data Prod")
                #to
                #cc
                #bcc
                emails_appended = []

                 
                data["personalizations"][0]["bcc"] = [];
                 
                 
                for email in recipients:
                    try:
                        if email.lower() != "devs@remotestaff.com.au":
                            if email.lower() not in emails_appended:
                                data["personalizations"][0]["to"].append({"email": email})
                                emails_appended.append(email.lower())
                                data_to_save["to"].append(email)
                    except Exception, e:
                        logging.info("Error appending recepients %s " % str(e))
                        pass

                     
                try:
                    if doc.has_key('cc'):
                        if doc['cc'] != None:
                            data["personalizations"][0]["cc"] = []
                            for email in doc['cc']:
                                if email.lower() not in emails_appended:
                                    data["personalizations"][0]["cc"].append({"email":email})
                                    emails_appended.append(email.lower())
                                    data_to_save["cc"].append(email)
                except Exception, e:
                    logging.info("Error adding cc email %s " % str(e))
                    pass

                try:
                    if doc.has_key('bcc'):
                        if doc['bcc'] != None:
                            data["personalizations"][0]["bcc"] = []
                            for email in doc['bcc']:
                                if email.lower() not in emails_appended:
                                    data["personalizations"][0]["bcc"].append({"email": email})
                                    data_to_save["bcc"].append(email)
                                    emails_appended.append(email.lower())
                except Exception, e:
                    logging.info("Error appending bcc %s " % str(e))
                    pass

                 
                devs_bcc_email = "devs@remotestaff.com.au"
                 
                if devs_bcc_email.lower() not in emails_appended:
                    data["personalizations"][0]["bcc"].append({"email":devs_bcc_email})
                    
       
            try:
                if doc.has_key('_attachments'):
                    data["attachments"] = []
                    for attachment in doc['_attachments']:
                        f = db.get_attachment(doc, attachment)
                        
                        
                        file_attachment = {
                                           "content": base64.b64encode(f.read()),
                                           "filename": attachment
                                           }
                        data["attachments"].append(file_attachment)
            except Exception, e:
                logging.info("Error attaching file %s " % str(e))
                pass
            
            try:
                api_url = settings.NODE_JS_API_URL+"/invoice/save-delivered-email-invoice";    
                
                import pycurl    
                import certifi
                try:
                    from urllib.parse import urlencode
                except:
                    from urllib import urlencode
                
                curl = pycurl.Curl()
                curl.setopt(curl.URL, api_url)
                post_data = { 'order_id': doc["order_id"], "doc_id": doc_id }
                postfields = urlencode(post_data)
                curl.setopt(curl.POSTFIELDS, postfields)
                curl.setopt(pycurl.SSL_VERIFYPEER, 1)
                curl.setopt(pycurl.SSL_VERIFYHOST, 2)
                curl.setopt(pycurl.CAINFO, certifi.where())
                curl.perform()
                curl.close() 
                logging.info("NJS URL %s " % api_url)
                
            except Exception, e:
                
                logging.info("Error sending njs %s " % str(e))
                pass
            
            #check if bcc, cc, and attachments has value 
            #if there is no value del
            if data["personalizations"][0].has_key("bcc"):
                if not data["personalizations"][0]["bcc"]:
                    del data["personalizations"][0]["bcc"]
                 
             
            if data["personalizations"][0].has_key("cc"):
                if not data["personalizations"][0]["cc"]:
                    del data["personalizations"][0]["cc"]
                 
            if data.has_key("attachments"):
                if not data["attachments"]:
                    del data["attachments"]
             
             
            #logging.info("SendGrid Data :%s" % data)
             
            response = sg.client.mail.send.post(request_body=data)
             
            
            is_invoice = True
    except NameError:
        logging.info("Not invoice!")
         
    #Injected By Josef Balisalisa End
    
    #add history that the document was send
    if is_invoice == False:
        try:
            #use mailgun
            logging.info("Sending via Mailgun :%s" % doc_id)
            s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
                port = settings.MAILGUN_CONFIG['port'])
            #s.starttls()
            s.login(settings.MAILGUN_CONFIG['username'],
                settings.MAILGUN_CONFIG['password']
                )
        
            s.sendmail(doc['from'],
                    recipients,
                    msg.as_string())
            s.quit()
            mailer = "Mailgun"
            logging.info("Email sent via Mailgun :%s" % doc_id)
        except:
            try:
                #use amozon ses
                logging.exception("Sending via Amazon SES :%s" % doc_id)
                s = smtplib.SMTP(host = settings.SMTP_CONFIG_SES_NEW['server'],
                    port = settings.SMTP_CONFIG_SES_NEW['port'])
                s.starttls()
                s.login(settings.SMTP_CONFIG_SES_NEW['username'],
                    settings.SMTP_CONFIG_SES_NEW['password']
                    )
            
                s.sendmail(doc['from'],
                        recipients,
                        msg.as_string())
                s.quit()
                mailer = "SES"
                logging.exception("Email send via Amazon SES :%s" % doc_id)
            except:
                logging.info("Failed sending email :%s" % doc_id)
        
    history = doc.get('history')
    if history == None:
        history = []

    history.append('%s sent by celery process, mailbox.send' % get_ph_time())
    
    while True:
        try:
            doc['history'] = history
            doc['sent'] = True
            doc['mailer'] = mailer
            doc['date_sent'] = get_ph_time(True)
            db.save(doc)
            break
        except:
            doc = db.get(doc_id)
    return

def run_tests():
    """
    >>> send('fail_doc')
    Traceback (most recent call last):
        ...
    Exception: ('Failed to Send Invoice', 'fail_doc not found')

##~    >>> send('sample_email_doc')
    >>> send('sample_email_doc_with_in_reply_to')

    """

if __name__ == '__main__':
    import doctest
    doctest.testmod()