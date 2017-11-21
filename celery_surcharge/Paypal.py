#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-01-17  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a function to query the assigned csro email
#   -   send an email alert if the assigned csro has been removed

from celery.task import task, Task
from celery.execute import send_task
import string
import couchdb
from decimal import Decimal
from pprint import pprint, pformat

from datetime import datetime
from pytz import timezone

import re

from cgi import parse_qsl

import settings 
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

couch_server = couchdb.Server(settings.COUCH_DSN)
db_client_docs = couch_server['client_docs']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')


@task(ignore_result=True)
def charge_fees(doc_id):
    """given the doc_id, create fees
    """
    logging.info('checking document %s ' % doc_id)
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = couch_server['client_docs']
    doc = db_client_docs.get(doc_id)

    if doc == None:
        raise Exception('document %s not found for creating paypal fees' % doc_id)

    if doc.has_key('type') == False:
        raise Exception('%s doc has no type field' % doc_id)

    if doc['type'] != 'paypal_transaction':
        raise Exception('%s is not a secure_pay_transaction type doc' % doc_id)

    if doc.has_key('response') == False:
        raise Exception('%s does not have a response field' % doc_id)
    
    if doc.has_key('order_id') == False:
        raise Exception('%s does not have order_id field' % doc_id)

    #get leads_id
    order_id = doc['order_id']
    x = re.split('-', order_id)
    leads_id = int(x[0])

    response = dict(parse_qsl(doc['response']))
    if (('TOKEN' in response.keys()) and 
        ('ACK' in response.keys()) and 
        ('PAYMENTINFO_0_AMT' in response.keys()) and
        ('PAYMENTINFO_0_FEEAMT' in response.keys()) and
        ('PAYMENTINFO_0_TRANSACTIONID' in response.keys()) and
        (response['ACK'] == 'Success')):
        amount = Decimal(response['PAYMENTINFO_0_AMT'])
        charge = Decimal(response['PAYMENTINFO_0_FEEAMT'])
        paypal_transaction_id = response['PAYMENTINFO_0_TRANSACTIONID']
    else:
        raise Exception('%s does not seem to be a paid paypal transaction' % doc_id)

    now = __get_phil_time__(as_array = True)

    particular = 'Paypal Service Fee Ex GST for transaction %s' % (paypal_transaction_id)
    remarks = 'Payment amount of %s. Payment fee of %s. Tax Invoice No. %s' % (amount, charge, order_id)


    r = db_client_docs.view('client/running_balance', key=leads_id)

    if len(r.rows) == 0:
        running_balance = Decimal('0')
    else:                                                               
        running_balance = Decimal('%s' % r.rows[0].value)

    running_balance -= charge

    doc_transaction = dict(
        added_by = 'automatic charge on payment (celery_surcharge:Paypal.charge_fees)',
        added_on = now,
        charge = '%0.2f' % charge,
        client_id = leads_id,                                          
        credit = '0.00',
        credit_type = 'PAYPAL_FEE',
        currency = doc['currency'],
        remarks = remarks,
        type = 'credit accounting',
        running_balance = '%0.2f' % running_balance,
        particular = particular,
        paypal_doc_id = doc_id,
    )

    db_client_docs.save(doc_transaction)
	
    doc['mongo_synced'] = False	
    db_client_docs.save(doc)	
	
    send_task('notify_devs.send', ['PAYPAL FEE EXTRACTED', 'Please check invoice %s, paypal fee:%s' % (order_id, charge)])  #TODO delete this notification once stable


def __get_phil_time__(as_array = False):
    utc_tz = timezone('UTC')
    ph_tz = timezone('Asia/Manila')
    now = utc_tz.localize(datetime.utcnow()).astimezone(ph_tz)

    #set date without timezone
    if as_array:
        now = [now.year, now.month, now.day,
            now.hour, now.minute, now.second, now.microsecond]
    else:
        now = datetime(now.year, now.month, now.day,
            now.hour, now.minute, now.second, now.microsecond)
    return now



if __name__ == '__main__':
##~    charge_fees('FAIL')
    charge_fees('fc4e93fa9aa54d2a05854eb7aa88dad3')
    charge_fees('7e30ca1c5f678e0ff1893e55f8f0a3c2')
##~    charge_fees('eb554915be940918aaf7dc88572fdad4')
##~    charge_fees('54ab73340c45f8a48aca35f21d2efea5')
    charge_fees('50b18594d811b55cfe7cd4600af29cef')
