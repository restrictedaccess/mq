#   2013-06-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used task for retrieving address_to
#   2013-05-21  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add sent_flag field to flag manually created invoice if it needs to be sent when client reaches 5 days to 0
#   2013-05-14  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   convert sending of email to couchdb mailbox on notifications of autodebit
#   2013-04-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3758, replace number of days with the actual date
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered auto generated invoice due to overpayment
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2013-01-24 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add credit card form link, updated some text
#   2013-01-10 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   use explanatory notes on invoice only for -30 days group
#   -   add date coverage explanatory notes for non -30 days group
#   2013-01-03 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix subject line for invoice, do not consider checking dates on items that are Adjustments
#   -   fix checking earliest and latest dates
#   2012-12-21 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   notify accounts for autodebit clients, wf 3419
#   2012-12-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3410, update invoice subject
#   2012-12-07 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add retries on saving history
#   2012-12-06 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add logging to monitor document conflicts
#   2012-12-04 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   send different email message, workflow 3330
#   2012-11-30 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   attach the message sent to the document 
#   2012-11-29 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add function for sending a separate email for 2 days before 0
#   -   and 0 days
#   2012-11-06 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix for rackspace, yahoo html invoice email formatting
#   2012-10-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   extended to show payment mode
#   2012-10-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   display running balance 0 when clients running balance is negative
#   2012-10-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fixes for unicode pound sign
#   2012-10-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   integrated attachment of pdf invoice
#   2012-09-28  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated texts ORDER to INVOICE  #TODO commit
#   2012-09-27  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered paid invoices
#   2012-09-19  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a task to send custom message invoice

import settings
import couchdb
import re
import string
from pprint import pformat

from persistent_mysql_connection import engine
from sqlalchemy.sql import text
import pika
from celery.task import task, Task
from celery.execute import send_task

from Cheetah.Template import Template
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal

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

import certifi

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


def get_email_subject(doc):
    """given the invoice doc, return a string that will correspond
    to date coverage and due date
    """
    earliest_date = False
    latest_date = False
    has_items = False
    
    if doc.has_key('items'):
        has_items = True
    else:
        return 'Your RemoteStaff Invoice # %s' % doc['order_id']

    if has_items == True:
        for item in doc['items']:
            #dont consider date if adjustment
            if string.find(item['description'], 'Adjustment') != -1:
                continue

            #check start_date
            if item.has_key('start_date'):
                x = item['start_date']
                start_date = date(x[0], x[1], x[2])
                if earliest_date == False:
                    earliest_date = start_date
                else:
                    if earliest_date > start_date:
                        earliest_date = start_date

            #check end_date
            if item.has_key('end_date'):
                x = item['end_date']
                end_date = date(x[0], x[1], x[2])
                if latest_date == False:
                    latest_date = end_date
                else:
                    if latest_date < end_date:
                        latest_date = end_date

    if earliest_date == False or latest_date == False:
        return 'Your RemoteStaff Invoice # %s' % doc['order_id']
    else:
        subject = 'Your RemoteStaff Invoice for %s - %s.' % (earliest_date.strftime('%B %d, %Y'), latest_date.strftime('%B %d, %Y'))

    if doc.has_key('pay_before_date'):
        x = doc['pay_before_date']
        pay_before_date = date(x[0], x[1], x[2])
        subject += ' Due on %s.' % pay_before_date.strftime('%B %d, %Y')

    return subject


@task
def get_invoice_html(doc_id):
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to retrieve invoice', '%s not found' % doc_id)

    html = generate_html(doc, '$items')
    return html



@task
def generate_html_header_and_items(doc):
    """given the doc, return an html of the headers and items
    """
    t = Template(file="templates/invoice_header_and_items.tmpl")

    #get currency_sign
    sql = text("""SELECT sign from currency_lookup
        WHERE code = :currency
        """)
    conn = engine.connect()
    currency_sign = conn.execute(sql, currency = doc['currency']).fetchone()['sign']
    t.currency_sign = currency_sign

    #get client_data
    sql = text("""SELECT company_name, company_address
        FROM leads
        WHERE id = :client_id
        """)

    client_data = conn.execute(sql, client_id = doc['client_id']).fetchone()
    t.company_name = client_data['company_name']
    t.company_address = client_data['company_address']

    t.show_payment_mode = False
    t.payment_mode = False
    t.input_amount = False
    if doc.has_key('show_payment_mode'):    #check if we want to show payment mode
        if doc['show_payment_mode'] == True:
            t.show_payment_mode = True
            t.payment_mode = doc['payment_mode']
            input_amount = Decimal(doc['input_amount'])
            t.input_amount = locale.format('%0.2f', input_amount, True)


    items_converted_date = []
    items = doc['items']
    flag_has_date = False

    for item in items:
        a = item.copy()
        if a.has_key('start_date'):
            b = a['start_date']
            a['start_date'] = date(b[0], b[1], b[2])
            flag_has_date = True
        if a.has_key('end_date'):
            c = a['end_date']
            a['end_date'] = date(c[0], c[1], c[2])
            flag_has_date = True

        amount = Decimal(a['amount'])
        a['amount'] = locale.format('%0.2f', amount, True)

        a['qty'] = locale.format('%0.2f', Decimal(a['qty']), True)
        a['unit_price'] = locale.format('%0.2f', Decimal(a['unit_price']), True)

        items_converted_date.append(a)


    t.flag_has_date = flag_has_date
    task_client_name = send_task('EmailSender.GetAddressTo', [doc['client_id']])
    t.client_name = task_client_name.get()

    t.invoice_items = items_converted_date
    t.order_id = doc['order_id']
    t.sub_total = locale.format('%0.2f', Decimal(doc['sub_total']), True)
    t.currency = doc['currency']
    t.gst_amount = locale.format('%0.2f', Decimal(doc['gst_amount']), True)
    t.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)
    t.doc_id = doc['_id']

    conn.close()
    return '%s' % t


def generate_html(doc, invoice_header_and_items):
    """given the doc, return an html message for email
    """

    #get the template
    t = Template(file="templates/prepaid_invoice_message.tmpl")

    #assign variables to the template file
    task_client_name = send_task('EmailSender.GetAddressTo', [doc['client_id']])
    t.client_name = task_client_name.get()
    t.status = doc['status']
    t.client_id = doc['client_id']

    if doc['status'] == 'paid': #retrieve clients running balance
        #couchdb settings
        s = couchdb.Server(settings.COUCH_DSN)
        db_client_docs = s['client_docs']

        r = db_client_docs.view('client/running_balance', key=doc['client_id'])
        
        if len(r.rows) == 0:
            running_balance = Decimal('0.00')
        else:
            running_balance = Decimal('%0.2f' % r.rows[0].value)

        #displays a running balance 0 when client has a negative running balance
        if running_balance <= Decimal('0.00'):
            running_balance = Decimal('0.00')

        t.running_balance = locale.format('%0.2f', running_balance, True)


    if doc.has_key('overpayment_from'):
        t.overpayment_from = doc['overpayment_from'] 
    else:
        t.overpayment_from = False


    if doc.has_key('pay_before_date'):
        x = doc['pay_before_date']
        pay_before_date = date(x[0], x[1], x[2])
        t.pay_before_date = pay_before_date.strftime('%B %d, %Y')
    else:
        t.pay_before_date = None


    #get currency_sign
    sql = text("""SELECT sign from currency_lookup
        WHERE code = :currency
        """)
    conn = engine.connect()
    currency_sign = conn.execute(sql, currency = doc['currency']).fetchone()['sign']
    t.currency_sign = currency_sign

    #get client_data
    task_client_data = send_task("ClientsWithPrepaidAccounts.get_client_details", [doc['client_id'],])
    client_data = task_client_data.get()
    t.company_name = client_data['company_name']
    t.company_address = client_data['company_address']
    t.days_before_suspension = client_data['days_before_suspension']

    #check if invoice items has dates
    items = doc['items']
    flag_has_date = False

    for item in items:
        a = item.copy()
        if a.has_key('start_date') == True or a.has_key('end_date') == True:
            flag_has_date = True

    t.flag_has_date = flag_has_date

    t.order_id = doc['order_id']
    t.sub_total = locale.format('%0.2f', Decimal(doc['sub_total']), True)
    t.currency = doc['currency']
    t.gst_amount = locale.format('%0.2f', Decimal(doc['gst_amount']), True)
    t.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)
    t.DEBUG = settings.DEBUG
    t.doc_id = doc['_id']
    t.invoice_header_and_items = invoice_header_and_items

    conn.close()
    return '%s' % t

@task(ignore_result=True)
def send(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    attachment = 'message-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
        by = 'task celery prepaid_send_invoice.send'
    ))

    doc['history'] = history
    
    doc['email_to_list'] = email_to_list
    doc['email_cc_list'] = email_cc_list   
    doc['email_bcc_list'] = email_bcc_list 
    doc['email_from'] = email_from
    doc['subject'] = get_email_subject(doc)
    doc['mongo_synced'] = False    
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0
    if doc.has_key('days_running_low'):
        del doc['days_running_low']
            
    db.save(doc)
    
    logging.info('Sending invoice via API')
    try:
        api_url = settings.API_URL+"/prepaid-invoice/send-invoice/";    
        import pycurl    
        try:
            from urllib.parse import urlencode
        except:
            from urllib import urlencode
        
        curl = pycurl.Curl()
        curl.setopt(curl.URL, api_url)
        post_data = { 'doc_id': doc_id }
        postfields = urlencode(post_data)
        curl.setopt(curl.POSTFIELDS, postfields)
        curl.setopt(pycurl.SSL_VERIFYPEER, 1)
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        curl.setopt(pycurl.CAINFO, certifi.where())
        curl.perform()
        curl.close() 
    except:
        pass
            
    return 'ok'

@task(ignore_result=True)
def send_Original_Script(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    #Renaming this task
    #Remove the word Original_Script 
    
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    invoice_header_and_items = generate_html_header_and_items(doc)
    html_message = generate_html(doc, invoice_header_and_items)

    msg = MIMEMultipart()
    part1 = MIMEText('%s' % html_message, 'html', 'UTF-8')
    part1.add_header('Content-Disposition', 'inline')
    msg.attach(part1)

    #attach the invoice html
    invoice_attachment = MIMEText("""
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<style>
p {margin-top: 1em;}
</style>
    %s""" % invoice_header_and_items, 'html', 'UTF-8')
    invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc['order_id'])
    msg.attach(invoice_attachment)

    #attach the pdf invoice
    task = send_task('generate_pdf_invoice.generate', [doc_id,])
    result = task.get()
    pdf_attachment = MIMEApplication(result)
    pdf_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.pdf' % doc['order_id'])
    msg.attach(pdf_attachment)

    msg['To'] = string.join(email_to_list, ',')
    recipients = []
    for email in email_to_list:
        recipients.append(email)

    if email_cc_list != None:
        if len(email_cc_list) > 0:
            for email in email_cc_list:
                recipients.append(email)

            msg['Cc'] = string.join(email_cc_list, ',')

    if email_bcc_list != None:
        if len(email_bcc_list) > 0:
            for email in email_bcc_list:
                recipients.append(email)

    msg['From'] = email_from

    subject = get_email_subject(doc)

    if settings.DEBUG or settings.STAGING:
        subject = 'TEST %s' % subject
        recipients = [settings.EMAIL_ALERT]

    msg['Subject'] = subject
    logging.info("Sending using Mailgun %s" % doc_id)  
    
    #use mailgun
    s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
        port = settings.MAILGUN_CONFIG['port'])
    s.login(settings.MAILGUN_CONFIG['username'],
        settings.MAILGUN_CONFIG['password']
        )
    s.starttls()
    s.sendmail('accounts@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()
    print recipients
    logging.info("Sending email from prepaid_send_invoice.send succeeded using Mailgun %s" % doc_id)  

    #add history that the document was sent
    doc = db.get(doc_id)
    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    attachment = 'message-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s. Attached doc %s' % (email_to_list, email_cc_list, email_bcc_list, email_from, attachment),
        by = 'task celery prepaid_send_invoice.send'
    ))

    doc['history'] = history
    doc['mongo_synced'] = False	
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0
    db.save(doc)

    #attach the sent email
    while True:
        try:
            db.put_attachment(doc, '%s' % html_message, attachment)
            break
        except:
            doc = db.get(doc_id)
            
    return 'ok'

@task(ignore_result=True)
def send_custom_message(doc_id, rst, email_to_list, email_cc_list, email_bcc_list, html_message, email_from):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
        by = 'task celery prepaid_send_invoice.send_custom_message'
    ))

    doc['history'] = history
    doc['email_to_list'] = email_to_list
    doc['email_cc_list'] = email_cc_list   
    doc['email_bcc_list'] = email_bcc_list 
    doc['email_from'] = email_from
    doc['subject'] = get_email_subject(doc)
    doc['mongo_synced'] = False    
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0    
    db.save(doc)
    
    logging.info('Sending invoice via API')
    try:
        api_url = settings.API_URL+"/prepaid-invoice/send-invoice/";    
        import pycurl    
        try:
            from urllib.parse import urlencode
        except:
            from urllib import urlencode
        
        curl = pycurl.Curl()
        curl.setopt(curl.URL, api_url)
        post_data = { 'doc_id': doc_id, 'html_message' : html_message }
        postfields = urlencode(post_data)
        curl.setopt(curl.POSTFIELDS, postfields)
        curl.setopt(pycurl.SSL_VERIFYPEER, 1)
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        curl.setopt(pycurl.CAINFO, certifi.where())
        curl.perform()
        curl.close() 
    except:
        pass
            
    return 'ok'

@task(ignore_result=True)
def send_custom_message_Original_Script(doc_id, rst, email_to_list, email_cc_list, email_bcc_list, html_message, email_from):
    """given the doc_id, send email
    """
    html_message += """
<pre style="font-size:12px;padding-top:128px;">
-----------------------------------------
Ref doc: %s
-----------------------------------------
</pre>
    """ % doc_id
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    msg = MIMEMultipart()
    part1 = MIMEText('%s' % html_message, 'html', 'UTF-8')
    part1.add_header('Content-Disposition', 'inline')
    msg.attach(part1)

    #attach the invoice html
    invoice_header_and_items = generate_html_header_and_items(doc)
    invoice_attachment = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % invoice_header_and_items, 'html', 'UTF-8')
    invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc['order_id'])
    msg.attach(invoice_attachment)

    #attach the pdf invoice
    task = send_task('generate_pdf_invoice.generate', [doc_id,])
    result = task.get()
    pdf_attachment = MIMEApplication(result)
    pdf_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.pdf' % doc['order_id'])
    msg.attach(pdf_attachment)

    msg['To'] = string.join(email_to_list, ',')
    recipients = []
    for email in email_to_list:
        recipients.append(email)

    if email_cc_list != None:
        if len(email_cc_list) > 0:
            for email in email_cc_list:
                recipients.append(email)

            msg['Cc'] = string.join(email_cc_list, ',')

    if email_bcc_list != None:
        if len(email_bcc_list) > 0:
            for email in email_bcc_list:
                recipients.append(email)

    msg['From'] = email_from

    subject = get_email_subject(doc)

    if settings.DEBUG or settings.STAGING:
        subject = 'TEST %s' % subject
        recipients = [settings.EMAIL_ALERT]

    msg['Subject'] = subject

    s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
        port = settings.MAILGUN_CONFIG['port'])
    s.login(settings.MAILGUN_CONFIG['username'],
        settings.MAILGUN_CONFIG['password']
        )

    s.sendmail('accounts@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()    
    logging.info("Sending email using Mailgun %s" % doc_id)

    #add history that the document was sent
    doc = db.get(doc_id)
    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    attachment = 'custom-message-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s. Attached doc %s' % (email_to_list, email_cc_list, email_bcc_list, email_from, attachment),
        by = 'task celery prepaid_send_invoice.send_custom_message'
    ))

    doc['history'] = history
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0
    doc['mongo_synced'] = False	
    db.save(doc)

    #attach the sent email
    db.put_attachment(doc, '%s' % html_message, attachment)

    return 'ok'

@task(ignore_result=True)
def send_2_days_to_0(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    attachment = 'message-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
        by = 'task celery prepaid_send_invoice.send_2_days_to_0'
    ))

    doc['history'] = history
    
    doc['email_to_list'] = email_to_list
    doc['email_cc_list'] = email_cc_list   
    doc['email_bcc_list'] = email_bcc_list 
    doc['email_from'] = email_from
    doc['subject'] = 'Invoice Payment Follow Up [%s]' % doc['order_id']
    doc['mongo_synced'] = False    
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0
    doc['days_running_low'] = "2"
    db.save(doc)
    
    logging.info('Sending invoice via API')
    try:
        api_url = settings.API_URL+"/prepaid-invoice/send-invoice/";    
        import pycurl    
        try:
            from urllib.parse import urlencode
        except:
            from urllib import urlencode
        
        curl = pycurl.Curl()
        curl.setopt(curl.URL, api_url)
        post_data = { 'doc_id': doc_id }
        postfields = urlencode(post_data)
        curl.setopt(curl.POSTFIELDS, postfields)
        curl.setopt(pycurl.SSL_VERIFYPEER, 1)
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        curl.setopt(pycurl.CAINFO, certifi.where())
        curl.perform()
        curl.close() 
    except:
        pass
            
    return 'ok'

@task(ignore_result=True)
def send_2_days_to_0_Original_Script(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice from send_2_days_to_0', '%s not found' % doc_id)

    t = Template(file="templates/prepaid_invoice_2_days_before_0.tmpl")

    #assign variables to the template file
    conn = engine.connect()
    task_client_name = send_task('EmailSender.GetAddressTo', [doc['client_id']])
    t.client_name = task_client_name.get()

    t.order_id = doc['order_id']
    t.currency = doc['currency']
    sql = text("""SELECT sign from currency_lookup
        WHERE code = :currency
        """)
    currency_sign = conn.execute(sql, currency = doc['currency']).fetchone()['sign']
    t.currency_sign = currency_sign
    t.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)
    t.doc_id = doc['_id']

    #get the CSRO
    task = send_task('EmailSender.GetCsroDetails', [doc['client_id'],])
    csro = task.get()
    t.admin_fname = csro.admin_fname
    t.admin_lname = csro.admin_lname
    t.signature_notes = csro.signature_notes
    t.signature_contact_nos = csro.signature_contact_nos
    t.signature_company = csro.signature_company
    t.signature_websites = csro.signature_websites

    #get client timezone for tracking date added
    sql = text("""SELECT t.timezone
        FROM leads AS l
        JOIN timezone_lookup AS t
        ON l.timezone_id = t.id
        WHERE l.id = :client_id
    """)

    data = conn.execute(sql, client_id=doc['client_id']).fetchone()
    if data == None:
        tz_string = 'Australia/Sydney'
    else:
        tz_string = data['timezone']

    ph_tz = timezone('Asia/Manila')
    client_tz = timezone(tz_string)
    a = doc['added_on']
    added_on = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=ph_tz)
    added_on_client_tz = added_on.astimezone(client_tz)
    t.added_on_client_tz = added_on_client_tz.strftime('%B %d, %Y')

    msg = MIMEMultipart()
    part1 = MIMEText('%s' % t, 'html', 'UTF-8')
    part1.add_header('Content-Disposition', 'inline')
    msg.attach(part1)

    #attach the invoice html
    invoice_header_and_items = generate_html_header_and_items(doc)
    invoice_attachment = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % invoice_header_and_items, 'html', 'UTF-8')
    invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc['order_id'])
    msg.attach(invoice_attachment)

    #attach the pdf invoice
    task = send_task('generate_pdf_invoice.generate', [doc_id,])
    result = task.get()
    pdf_attachment = MIMEApplication(result)
    pdf_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.pdf' % doc['order_id'])
    msg.attach(pdf_attachment)

    msg['To'] = string.join(email_to_list, ',')
    recipients = []
    for email in email_to_list:
        recipients.append(email)

    if email_cc_list != None:
        if len(email_cc_list) > 0:
            for email in email_cc_list:
                recipients.append(email)

            msg['Cc'] = string.join(email_cc_list, ',')

    if email_bcc_list != None:
        if len(email_bcc_list) > 0:
            for email in email_bcc_list:
                recipients.append(email)

    msg['From'] = email_from

    subject = 'Invoice Payment Follow Up [%s]' % doc['order_id']

    if settings.DEBUG or settings.STAGING:
        subject = 'TEST %s' % subject
        recipients = [settings.EMAIL_ALERT]

    msg['Subject'] = subject
    
    s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
        port = settings.MAILGUN_CONFIG['port'])
    s.login(settings.MAILGUN_CONFIG['username'],
        settings.MAILGUN_CONFIG['password']
        )

    s.sendmail('accounts@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()    
    logging.info("Sending email using Mailgun %s" % doc_id)    
        
    #add history that the document was sent
    doc_saved = False
    while doc_saved == False:
        try:
            doc = db.get(doc_id)
            logging.info('updating %s rev %s' % (doc_id, doc['_rev']))

            if doc.has_key('history') == False:
                history = []
            else:
                history = doc['history']

            now = get_ph_time()
            attachment = 'payment-follow-up-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

            history.append(dict(
                timestamp = now.strftime('%F %H:%M:%S'),
                changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s. Attached doc %s' % (email_to_list, email_cc_list, email_bcc_list, email_from, attachment),
                by = 'task celery prepaid_send_invoice.send_2_days_to_0'
            ))

            doc['history'] = history
            doc['mongo_synced'] = False 			
            db.save(doc)

            #attach the sent email
            db.put_attachment(doc, '%s' % t, attachment)

            doc_saved = True

        except couchdb.http.ResourceConflict:
            send_task('notify_devs.send', ['DOCUMENT CONFLICT on send_2_days_to_0', 'Failed to save client_docs %s with rev %s. Retrying...' % (doc_id, doc['_rev'])])

    conn.close()
    return 'ok'

@task(ignore_result=True)
def send_0_to_negative(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    if doc.has_key('history') == False:
        history = []
    else:
        history = doc['history']

    now = get_ph_time()
    attachment = 'message-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

    history.append(dict(
        timestamp = now.strftime('%F %H:%M:%S'),
        changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
        by = 'task celery prepaid_send_invoice.send_0_to_negative'
    ))

    doc['history'] = history
    
    doc['email_to_list'] = email_to_list
    doc['email_cc_list'] = email_cc_list   
    doc['email_bcc_list'] = email_bcc_list 
    doc['email_from'] = email_from
    doc['subject'] = 'Invoice Payment # %s Is Overdue' % doc['order_id']
    doc['mongo_synced'] = False    
    doc['sent_flag'] = 'Y'  #flag to trackdown newly generated invoice, sending during 5 days before 0
    doc['days_running_low'] = "0"
    db.save(doc)
    
    logging.info('Sending invoice via API')
    try:
        api_url = settings.API_URL+"/prepaid-invoice/send-invoice/";    
        import pycurl    
        try:
            from urllib.parse import urlencode
        except:
            from urllib import urlencode
        
        curl = pycurl.Curl()
        curl.setopt(curl.URL, api_url)
        post_data = { 'doc_id': doc_id }
        postfields = urlencode(post_data)
        curl.setopt(curl.POSTFIELDS, postfields)
        curl.setopt(pycurl.SSL_VERIFYPEER, 1)
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        curl.setopt(pycurl.CAINFO, certifi.where())
        curl.perform()
        curl.close() 
    except:
        pass
            
    return 'ok'

@task(ignore_result=True)
def send_0_to_negative_Original_Script(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
    """send invoice task when the clients running balance is less than or equal to 0
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice from send_0_to_negative', '%s not found' % doc_id)

    t = Template(file="templates/prepaid_invoice_0_to_negative.tmpl")

    #assign variables to the template file
    conn = engine.connect()
    task_client_name = send_task('EmailSender.GetAddressTo', [doc['client_id']])
    t.client_name = task_client_name.get()

    t.order_id = doc['order_id']
    t.currency = doc['currency']
    sql = text("""SELECT sign from currency_lookup
        WHERE code = :currency
        """)
    currency_sign = conn.execute(sql, currency = doc['currency']).fetchone()['sign']
    t.currency_sign = currency_sign
    t.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)
    t.doc_id = doc['_id']

    #get the CSRO
    task = send_task('EmailSender.GetCsroDetails', [doc['client_id'],])
    csro = task.get()
    t.admin_fname = csro.admin_fname
    t.admin_lname = csro.admin_lname
    t.signature_notes = csro.signature_notes
    t.signature_contact_nos = csro.signature_contact_nos
    t.signature_company = csro.signature_company
    t.signature_websites = csro.signature_websites

    #get client timezone for tracking date added
    sql = text("""SELECT t.timezone
        FROM leads AS l
        JOIN timezone_lookup AS t
        ON l.timezone_id = t.id
        WHERE l.id = :client_id
    """)

    data = conn.execute(sql, client_id=doc['client_id']).fetchone()
    if data == None:
        tz_string = 'Australia/Sydney'
    else:
        tz_string = data['timezone']

    ph_tz = timezone('Asia/Manila')
    client_tz = timezone(tz_string)
    a = doc['added_on']
    added_on = datetime(a[0], a[1], a[2], a[3], a[4], a[5], tzinfo=ph_tz)
    added_on_client_tz = added_on.astimezone(client_tz)
    t.added_on_client_tz = added_on_client_tz.strftime('%B %d, %Y')

    msg = MIMEMultipart()
    part1 = MIMEText('%s' % t, 'html', 'UTF-8')
    part1.add_header('Content-Disposition', 'inline')
    msg.attach(part1)

    #attach the invoice html
    invoice_header_and_items = generate_html_header_and_items(doc)
    invoice_attachment = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % invoice_header_and_items, 'html', 'UTF-8')
    invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc['order_id'])
    msg.attach(invoice_attachment)

    #attach the pdf invoice
    task = send_task('generate_pdf_invoice.generate', [doc_id,])
    result = task.get()
    pdf_attachment = MIMEApplication(result)
    pdf_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.pdf' % doc['order_id'])
    msg.attach(pdf_attachment)

    msg['To'] = string.join(email_to_list, ',')
    recipients = []
    for email in email_to_list:
        recipients.append(email)

    if email_cc_list != None:
        if len(email_cc_list) > 0:
            for email in email_cc_list:
                recipients.append(email)

            msg['Cc'] = string.join(email_cc_list, ',')

    if email_bcc_list != None:
        if len(email_bcc_list) > 0:
            for email in email_bcc_list:
                recipients.append(email)

    msg['From'] = email_from

    subject = 'Invoice Payment # %s Is Overdue' % doc['order_id']

    if settings.DEBUG or settings.STAGING:
        subject = 'TEST %s' % subject
        recipients = [settings.EMAIL_ALERT]

    msg['Subject'] = subject

    s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
        port = settings.MAILGUN_CONFIG['port'])
    s.login(settings.MAILGUN_CONFIG['username'],
        settings.MAILGUN_CONFIG['password']
        )

    s.sendmail('accounts@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()    
    logging.info("Sending email using Mailgun %s" % doc_id)
    
    #add history that the document was sent
    doc_saved = False
    while doc_saved == False:
        try:
            doc = db.get(doc_id)
            logging.info('updating %s rev %s' % (doc_id, doc['_rev']))

            if doc.has_key('history') == False:
                history = []
            else:
                history = doc['history']

            now = get_ph_time()
            attachment = 'overdue-%s:%s.html' % (now.strftime('%F %H:%M:%S'), now.microsecond)

            history.append(dict(
                timestamp = now.strftime('%F %H:%M:%S'),
                changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s. Attached doc %s' % (email_to_list, email_cc_list, email_bcc_list, email_from, attachment),
                by = 'task celery prepaid_send_invoice.send_0_to_negative'
            ))

            doc['history'] = history
            doc['mongo_synced'] = False 			
            db.save(doc)

            #attach the sent email
            db.put_attachment(doc, '%s' % t, attachment)

            doc_saved = True

        except couchdb.http.ResourceConflict:
            send_task('notify_devs.send', ['DOCUMENT CONFLICT on send_0_to_negative', 'Failed to save client_docs %s with rev %s. Retrying...' % (doc_id, doc['_rev'])])

    conn.close()
    return 'ok'


@task(ignore_result=True)
def notify_accounts_autodebit(doc_invoice):
    subject = 'direct debit client %s %s invoiced, please process' % (doc_invoice['client_fname'], doc_invoice['client_lname'])

    message = """Hi Accounts,

FYI %s %s : %s a direct debit client has been invoiced the
invoice number %s. Proceed to process payment ASAP and update invoice.
""" % (doc_invoice['client_fname'], doc_invoice['client_lname'], doc_invoice['client_id'], doc_invoice['order_id'])

    doc = dict(
        to = ['accounts@remotestaff.com.au'],
        bcc = ['devs@remotestaff.com.au'],
        created = get_ph_time(as_array=True),
        generated_by = 'celery task prepaid_send_invoice.notify_accounts_autodebit',
        text = message,
        subject = subject,
        sent = False,
    )
    doc['from'] = "Celery Process Notify Autodebit Client<noreply@remotestaff.com.au>"

    s = couchdb.Server(settings.COUCH_DSN)
    db = s['mailbox']
    db.save(doc)


def run_tests():
    """
    >>> send_0_to_negative('5a2814d4f18a2ff4e9954624b2152129', ['Nihal.samara@kaizensynergy.com'], ['lance@remotestaff.com.au', 'accounts@remotestaff.com.au'], ['devs@remotestaff.com.au', 'staffing_consultant@remotestaff.com.au'], 'accounts@remotestaff.com.au')
    """
    

if __name__ == '__main__':
    import doctest
    doctest.testmod() 