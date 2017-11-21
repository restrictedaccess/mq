#   2013-06-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used task for retrieving address_to
#   2013-04-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated email receipt, attached the newly generated overpaid invoice (if overpaid) and disabled separate sending
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   generate new invoice when invoice is overpaid
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   activate creation of invoice due to overpayment
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2012-10-24  lawrence sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix to retrieve current running balance of client
#   2012-10-22  lawrence sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task to send receipt

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

from pprint import pprint, pformat

import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
import certifi

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
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)

def insert_invoice_payment_collection(doc_order):
    
    
    logging.info("prepaid_send_receipt.insert_invoice_payment_collection %s" % doc_order['_id'])
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
     
         
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017)                     
    mongodb = mongo_client.prod
    col = mongodb.invoice_payments
     
    client_id =  doc_order['client_id']
     
     
    now = get_ph_time(as_array=True)
    view = db.view('client/settings', startkey=[client_id, now],
        endkey=[client_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)
    doc_client_settings = view.rows[0].doc
   
    if doc_client_settings.has_key('days_before_suspension'):
        #days_before_suspension = Decimal('%s' % doc_client_settings['days_before_suspension'])
        days_before_suspension = doc_client_settings['days_before_suspension']
    else:
        days_before_suspension = 0
         
    if days_before_suspension == -30:
        billing_type = "monthly"
    else:
        billing_type = "prepaid"    
     
    remarks = None
    over_payment = True
    if over_payment:
        remarks = 'overpayment from Invoice # %s' % doc_order['overpayment_from']
 
    pay_before_date = None        
    if doc_order.has_key('pay_before_date'):
        pay_before_date = doc_order['pay_before_date']
        pay_before_date = datetime(pay_before_date[0], pay_before_date[1], pay_before_date[2], pay_before_date[3], pay_before_date[4], pay_before_date[5]) 
    
    invoice_date = None    
    if doc_order.has_key('added_on'):
        invoice_date = doc_order['added_on']
        invoice_date = datetime(invoice_date[0], invoice_date[1], invoice_date[2], invoice_date[3], invoice_date[4], invoice_date[5])        
    
    payment_date = None    
    if doc_order.has_key('payment_date'):
        payment_date = doc_order['payment_date']
        payment_date = datetime(payment_date[0], payment_date[1], payment_date[2], payment_date[3], payment_date[4], payment_date[5])    
                                 
    record={            
        'couch_id' : doc_order['_id'],
        'added_on' : get_ph_time(False),             
        'transaction_id' : None,
        'transaction_doc' : None,
        'client_id' : int(client_id),
        'payment_mode' : doc_order['payment_mode'],
        'order_id' : doc_order['order_id'],
        'invoice_date' : invoice_date,
        'pay_before_date' : pay_before_date,
        'input_amount' : doc_order['input_amount'],
        'total_amount' : doc_order['total_amount'],
        'currency' : doc_order['currency'],
        'payment_date' : payment_date,
        'days_before_suspension' : days_before_suspension,
        'billing_type' : billing_type,
        'remarks' : remarks, 
        'set_paid_by' : "system",
        'admin_id' : None,          
        'response' : None,
        'doc_order' : doc_order,
        'over_payment' : over_payment,
        'overpayment_from' : doc_order['overpayment_from'],
        'overpayment_from_doc_id' :  doc_order['overpayment_from_doc_id']
         
    }
    col.insert(record)
    send_receipt(doc_order['_id'])

@task
def create_overpayment_invoice(doc_id):
    """generate an invoice based on over payment
    return a document copy for further processing
    """
    
    logging.info("prepaid_send_receipt.create_overpayment_invoice %s" % doc_id)
    
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Create Over Payment Invoice', '%s not found' % doc_id)

    if doc.has_key('input_amount') == False:
        raise Exception('Failed to Create Over Payment Invoice', 'input_amount field for %s not found' % doc_id)

    leads_id = doc['client_id']

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    r = db.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    currency, apply_gst = r.rows[0]['value']

    #check clients running balance
    r = db.view('client/running_balance', key=leads_id)
    
    if len(r.rows) == 0:
        running_balance = Decimal('0.00')
    else:
        running_balance = Decimal('%0.2f' % r.rows[0].value)

    #get fname, lname
    sql = text("""SELECT fname, lname, email, registered_domain from leads
        WHERE id = :leads_id
        """)
    conn = engine.connect()
    client_fname, client_lname, client_email, registered_domain = conn.execute(sql, leads_id = leads_id).fetchone()

    #get last order id
    r = db.view('client/last_order_id', startkey=[leads_id, "%s-999999999" % leads_id],
        endkey=[leads_id, ""], descending=True, limit=1)

    if len(r.rows) == 0:
        last_order_id = 1
    else:
        last_order_id_str = r.rows[0].key[1]
        x, last_order_id = string.split(last_order_id_str, '-')
        last_order_id = int(last_order_id)
        last_order_id += 1

    order_id = '%s-%08d' % (leads_id, last_order_id)

    doc_total_amount = Decimal(doc['total_amount'])
    doc_input_amount = Decimal(doc['input_amount'])
    total_amount = doc_input_amount - doc_total_amount

    if apply_gst == 'Y':
        sub_total = total_amount / Decimal('1.1')
        gst_amount = total_amount - sub_total
    else:
        sub_total = total_amount
        gst_amount = Decimal('0.00')

    invoice_item = dict(
        item_id = 1,
        unit_price = '%0.2f' % sub_total,
        qty = '1.00',
        amount = '%0.2f' % sub_total,
        description = 'overpayment from Invoice # %s' % doc['order_id'],
    )
    invoice_items = [invoice_item]
    
    doc_order = dict(
        added_by = 'celery overpayment',
        apply_gst = apply_gst,
        client_id = leads_id,
        history = [],
        type = 'order',
        added_on = now,
        items = invoice_items,
        status = 'paid',
        date_paid = get_ph_time().strftime('%F %H:%M:%S'),
        payment_date = get_ph_time(as_array = True),
        order_id = order_id,
        sub_total = '%0.2f' % sub_total,
        input_amount = '%0.2f' % total_amount,
        total_amount = '%0.2f' % total_amount,
        gst_amount = '%0.2f' % gst_amount,
        client_fname = client_fname,
        client_lname = client_lname,
        client_email = client_email,
        registered_domain = registered_domain, 
        currency = currency,
        overpayment_from = doc['order_id'],
        overpayment_from_doc_id = doc['_id'],
        payment_mode = doc['payment_mode'],
        admin_id = None,
        admin_name = None,
        set_paid_by = "system"
    )

    doc_order['running_balance'] = '%0.2f' % running_balance

    db.save(doc_order)
    logging.info('created overpayment %s' % doc_order['_id'])
    
    insert_invoice_payment_collection(doc_order)
    
    #sync auto paid invoice to xero
    import pycurl
    c = pycurl.Curl()
    c.setopt(c.URL, settings.API_URL+'/mongo-index/sync-client-invoice-by-order-id/')
    try:
        # python 3
        from urllib.parse import urlencode
    except ImportError:
        # python 2
        from urllib import urlencode
    post_data = {'order_id': order_id, 'xero_sync':"1"}
    logging.info("Executing /mongo-index/sync-client-invoice-by-order-id?order_id=%s&xero_sync=1" % order_id)
    # Form data must be provided already urlencoded.
    postfields = urlencode(post_data)
    # Sets request method to POST,
    # Content-Type header to application/x-www-form-urlencoded
    # and data to send in request body.
    c.setopt(c.POSTFIELDS, postfields)
    
    c.perform()
    c.close()
    
    
    #send_task('notify_devs.send', ['Created Overpayment for Invoice %s' % doc_order['order_id'], 'Please check :\n%r' % doc_order.copy()])
    conn.close()
    return doc_order.copy()



def send_receipt(doc_id):
    
    logging.info("prepaid_send_receipt.send_receipt %s" % doc_id)    
    from StringIO import StringIO
    import pycurl
    
    url = settings.NJS_URL+"/send/send-payment-receipt/?couch_id="+str(doc_id)    
    storage = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.WRITEFUNCTION, storage.write)
    c.setopt(pycurl.SSL_VERIFYPEER, 1)
    c.setopt(pycurl.SSL_VERIFYHOST, 2)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.perform()
    c.close()       

@task(ignore_result=True)            
def send(doc_id):
    """given the doc_id, send email
    """
    
    logging.info("prepaid_send_receipt.send %s" % doc_id )
    
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
    
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Receipt', '%s not found' % doc_id)
    

    #check if we need to issue a separate invoice due to overpayment
    over_payment = False
    doc_total_amount = doc.get('total_amount')
    doc_input_amount = doc.get('input_amount')
    doc_overpayment = None
    overpayment_order_id = None

    if doc_total_amount != None and doc_input_amount != None:
        if round(float(doc_input_amount), 2) > round(float(doc_total_amount), 2):
            over_payment = True
            result = create_overpayment_invoice.delay(doc['_id'])
            doc_overpayment = result.get()
            overpayment_order_id = doc_overpayment['order_id']
            logging.info("Overpayment order_id : %s" % overpayment_order_id)
            
            
    #add history that the document was sent
    doc = db.get(doc_id)
    
    #If overpayment save the newly created doc_id for reference
    doc['over_payment'] = over_payment
    if doc_overpayment != None:        
        doc['doc_overpayment_id'] = doc_overpayment['_id']    
    doc['mongo_synced'] = False    
    db.save(doc)
    
    
    send_receipt(doc_id)        
    
    

#@task(ignore_result=True)
# def send(doc_id, email_to_list, email_cc_list, email_bcc_list, email_from):
#     """given the doc_id, send email
#     """
#     
#     top_up_url = "https://remotestaff.com.au/portal/v2/payments/top-up"
#     
#     if settings.DEBUG:
#        top_up_url = "http://devs.remotestaff.com.au/portal/v2/payments/top-up"
#     
#     if settings.STAGING:
#        top_up_url = "http://staging.remotestaff.com.au/portal/v2/payments/top-up"    
#     
#     
#     
#     #couchdb settings
#     s = couchdb.Server(settings.COUCH_DSN)
#     db = s['client_docs']
# 
#     doc = db.get(doc_id)
#     if doc == None:
#         raise Exception('Failed to Send Receipt', '%s not found' % doc_id)
# 
#     t = Template(file='templates/prepaid_send_receipt.tmpl')
# 
#     #check if we need to issue a separate invoice due to overpayment
#     over_payment = False
#     doc_total_amount = doc.get('total_amount')
#     doc_input_amount = doc.get('input_amount')
#     doc_overpayment = None
#     overpayment_order_id = None
#     
#     if doc_total_amount != None and doc_input_amount != None:
#         if Decimal(doc_input_amount) > Decimal(round(doc_total_amount, 2)):
#             over_payment = True
#             result = create_overpayment_invoice.delay(doc['_id'])
#             doc_overpayment = result.get()
#             overpayment_order_id = doc_overpayment['order_id']
#             logging.info("Overpayment order_id : %s" % overpayment_order_id)
#             
#                 
#     t.total_amount = doc.get('total_amount')
#     t.over_payment = over_payment
#     t.overpayment_order_id = overpayment_order_id
#     t.top_up_url = top_up_url
#     
#     #get currency_sign
#     sql = text("""SELECT code, sign from currency_lookup
#         WHERE code = :currency
#         """)
#     conn = engine.connect()
#     currency = conn.execute(sql, currency = doc['currency']).fetchone()
#     t.currency = currency
#     
#     
#     input_amount = Decimal(doc['input_amount'])        
#     t.input_amount = locale.format('%0.2f', input_amount, True)
#     
#     t.payment_mode = doc['payment_mode']
#     t.order_id = doc['order_id']
# 
#     #get clients running balance
#     r = db.view('client/running_balance', key=doc['client_id'])
#     
#     if len(r.rows) == 0:
#         running_balance = Decimal('0.00')
#     else:
#         running_balance = Decimal('%0.2f' % r.rows[0].value)
#     t.running_balance = locale.format('%0.2f', running_balance, True)
# 
#     #get client_data
#     task_client_name = send_task('EmailSender.GetAddressTo', [doc['client_id']])
#     client_name = task_client_name.get()
#     t.client_name = client_name
#     
#     #check if client is -30
#     now = get_ph_time(as_array = True)
#     r = db.view('client/settings', startkey=[doc['client_id'], now],
#         endkey=[doc['client_id'], [2011,1,1,0,0,0,0]], 
#         descending=True, limit=1, include_docs=True)
# 
#     if len(r.rows) == 0:    #no client settings, send alert
#         raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))
# 
#     data = r.rows[0]
#     client_settings_doc = data.doc
#     
#     negative_30_client = False
#     if client_settings_doc.has_key('days_before_suspension') == True:
#         if client_settings_doc['days_before_suspension'] == -30:
#             negative_30_client = True
#             logging.info("days_before_suspension : %s" % client_settings_doc['days_before_suspension'])
#     t.negative_30_client = negative_30_client  
#     logging.info("negative_30_client : %s" % t.negative_30_client)
#     
#     
#     doc_task = doc.copy()
#     doc_task['show_payment_mode'] = True
#     #doc_task['show_payment_mode'] = False
#     
#     #task = send_task("prepaid_send_invoice.generate_html_header_and_items", [doc_task])
#     #invoice_header_and_items = task.get()
#     
#     
#     msg = MIMEMultipart()
#     part1 = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % t, 'html', 'UTF-8')
#     part1.add_header('Content-Disposition', 'inline')
#     msg.attach(part1)
# 
#     #attach the invoice html
#     #invoice_attachment = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % invoice_header_and_items, 'html', 'UTF-8')
#     #invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc['order_id'])
#     #msg.attach(invoice_attachment)
# 
#     #if doc_overpayment != None:
#         #task = send_task("prepaid_send_invoice.generate_html_header_and_items", [doc_overpayment])
#         #overpayment_invoice_header_and_items = task.get()
# 
#         #overpayment_invoice_attachment = MIMEText('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />%s' % overpayment_invoice_header_and_items, 'html', 'UTF-8')
#         #overpayment_invoice_attachment.add_header('Content-Disposition', 'attachment', filename='invoice-%s.html' % doc_overpayment['order_id'])
#         #msg.attach(overpayment_invoice_attachment)
# 
#     msg['To'] = string.join(email_to_list, ',')
#     recipients = []
#     for email in email_to_list:
#         recipients.append(email)
# 
#     if email_cc_list != None:
#         if len(email_cc_list) > 0:
#             for email in email_cc_list:
#                 recipients.append(email)
# 
#             msg['Cc'] = string.join(email_cc_list, ',')
# 
#     if email_bcc_list != None:
#         if len(email_bcc_list) > 0:
#             for email in email_bcc_list:
#                 recipients.append(email)
# 
#     msg['From'] = email_from
# 
#     subject = 'REMOTE STAFF TOPUP PAYMENT RECEIPT # %s' % doc['order_id']
# 
#     if settings.DEBUG or settings.STAGING:
#         subject = 'TEST %s' % subject
#         recipients = [settings.EMAIL_ALERT]
# 
#     msg['Subject'] = subject
# 
#     #Send Invoice
#     try:
#         #use mailgun		
#         s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
#             port = settings.MAILGUN_CONFIG['port'])
#         #s.starttls()
#         s.login(settings.MAILGUN_CONFIG['username'],
#             settings.MAILGUN_CONFIG['password']
#             )
# 
#         s.sendmail('accounts@remotestaff.com.au', 
#             recipients,
#             msg.as_string())
#         s.quit()
#         logging.info("Sent via Mailgun %s" % doc_id)
# 				
#         
#     except:
# 	
#         s = smtplib.SMTP(host = settings.SMTP_CONFIG_SES['server'],
#             port = settings.SMTP_CONFIG_SES['port'])
#         s.starttls()
#         s.login(settings.SMTP_CONFIG_SES['username'],
#             settings.SMTP_CONFIG_SES['password']
#             )
# 
#         s.sendmail('accounts@remotestaff.com.au', 
#             recipients,
#             msg.as_string())
#         s.quit()
# 
#         logging.info("Sending email from prepaid_send_receipt.send succeeded using Amazon SES Old %s" % doc_id)
#             
#         
# 
#     #add history that the document was sent
#     doc = db.get(doc_id)
#     if doc.has_key('history') == False:
#         history = []
#     else:
#         history = doc['history']
# 
#     history.append(dict(
#         timestamp = get_ph_time().strftime('%F %H:%M:%S'),
#         changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
#         by = 'task celery prepaid_send_receipt.send'
#     ))
# 
#     doc['history'] = history
#     
#     #If overpayment save the newly created doc_id for reference
#     doc['over_payment'] = over_payment
#     if doc_overpayment != None:        
#         doc['doc_overpayment_id'] = doc_overpayment['_id']
#     
#     doc['mongo_synced'] = False    
#     db.save(doc)
# 
#     if doc_overpayment != None:
#         doc_overpayment = db.get(doc_overpayment['_id'])
#         history = doc.get('history')
#         if history == None:
#             history = []
# 
#         history.append(dict(
#             timestamp = get_ph_time().strftime('%F %H:%M:%S'),
#             changes = 'Email sent To: %s Cc:%s Bcc:%s From:%s.' % (email_to_list, email_cc_list, email_bcc_list, email_from),
#             by = 'task celery prepaid_send_receipt.send'
#         ))
# 
#         doc_overpayment['history'] = history
#         doc_overpayment['mongo_synced'] = False			
#         db.save(doc_overpayment)
# 
#     conn.close()


if __name__ == '__main__':
    logging.info('tests')
    send_receipt('9021c9bcecfb474e0a271fcea078472e')
    #send('7f7d36b27c4fdb8b4c17f4df638bfcef', ['test@remotestaff.com.au'], [], [], 'accounts@remotestaff.com.au')
    #send('425440c874fcb93a728dc718bd9d8cc4', ['test@remotestaff.com.au'], [], [], 'accounts@remotestaff.com.au')
    #send('0095c0ba8db23f818e36c650f275e9ac', ['test@remotestaff.com.au'], [], [], 'accounts@remotestaff.com.au')

    #create_overpayment_invoice('527a1d0fc1e3e8a3a70264697c3b851e')
    #create_overpayment_invoice('8dd62ac4e8e6f2adfa59df604783b8a4')
    #create_overpayment_invoice('8faad3bcc33bf4ee4826844b0ef178f3')
    #create_overpayment_invoice('10dc397fc314b910060b36fa98d9fdae')
