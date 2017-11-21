#   2013-08-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   moved sending emails to couchdb
#   2013-05-16  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed cristina.e@remotestaff.com.au as per rine/charlenes request
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2012-08-01
#   -   Added cristina.e@remotestaff.com.au as per ricas request

from celery.task import task, Task
from celery.task.sets import TaskSet
from celery.execute import send_task
import couchdb
from decimal import Decimal

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone
import calendar

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

import settings 
import logging
from pprint import pprint

import string
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def get_ph_time():
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


def get_csro_fname(email):
    """returns the first name given the email
    """
    sql = text("""SELECT admin_fname FROM admin
        WHERE admin_email=:email""")
    conn = engine.connect()
    r = conn.execute(sql, email=email).fetchone()
    conn.close()
    return string.capwords(string.strip(r.admin_fname))


@task(ignore_result=True)
class FirstMonthInvoicePayment(Task):
    def run(self, doc_id):
        logging.info('FirstMonthInvoicePayment %s' % (doc_id))
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['client_docs']
        doc = db.get(doc_id)

        #sanity checks
        if doc == None:
            logging.info('Error: document not found!')
            return
        if doc.has_key('type') == False:
            logging.info('Error: document has no type!')
            return
        if doc['type'] != 'order':
            logging.info('Error: document not an order type!')
            return
        if doc.has_key('added_by') == False:
            logging.info('Error: document has no field added_by!')
            return
        if doc['added_by'] != 'MQ First Months Invoice':
            logging.info('Error: document is not a first month invoice!')
            return

        client_id = doc['client_id']
        result = send_task("EmailSender.GetEmails", [client_id,])
        email_recipients = result.get()

        result = send_task("EmailSender.GetCsroEmail", [client_id,])
        csro_email = result.get()

        if csro_email == None:
            csro_fname = 'No Assigned CSRO'
            csro_email = 'csro@remotestaff.com.au'
        else:
            csro_fname = get_csro_fname(csro_email)

        #build the staff names
        staff_names = []
        for item in doc['items']:
            x = string.find(item['description'], ']')
            staff_names.append(item['description'][:x + 1])

        staff_names = string.join(staff_names, ', ')

        a = doc['earliest_starting_date']
        date_commence = datetime(a[0], a[1], a[2])

        #send email
        email_message = 'Dear %s,\n\nClient %s %s #%s <%s> has now paid prepaid invoice for %s.\n\nThe contract will commence on %s.\n\nPlease introduce yourself to both client and staff' % (csro_fname, doc['client_fname'], doc['client_lname'], doc['client_id'], doc['client_email'], staff_names, date_commence.strftime('%F'))

        to = ['devs@remotestaff.com.au', csro_email]
        subject = 'Client Pays First Month Invoice'

        #send email via mailbox
        db = s['mailbox']
        now = get_ph_time()
        created = [now.year, now.month, now.day, now.hour, now.minute, now.second]
        doc = dict(
            sent = False,
            created = created,
            generated_by = 'celery PrepaidCSROAutoResponders.FirstMonthInvoicePayment',
            text = email_message,
            subject = subject,
            to = to,
        )
        doc['from'] = 'noreply@remotestaff.com.au'
        db.save(doc)


@task(ignore_result=True)
class ClientRunningLowOnCredit(Task):
    def run(self, doc_id):
        logging.info('ClientRunningLowOnCredit %s' % (doc_id))
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['client_docs']
        doc = db.get(doc_id)

        #sanity checks
        if doc == None:
            logging.info('Error: document not found!')
            return
        if doc.has_key('type') == False:
            logging.info('Error: document has no type!')
            return
        if doc['type'] != 'order':
            logging.info('Error: document not an order type!')
            return

        client_id = doc['client_id']
        result = send_task("EmailSender.GetCsroEmail", [client_id,])
        csro_email = result.get()

        if csro_email == None:
            csro_fname = 'No Assigned CSRO'
            csro_email = 'csro@remotestaff.com.au'
        else:
            csro_fname = get_csro_fname(csro_email)

        #build the staff names
        staff_names = []
        for item in doc['items']:
            x = string.find(item['description'], ']')
            staff_names.append(item['description'][:x + 1])

        staff_names = string.join(staff_names, ', ')

        #send email
        email_message = 'Dear %s,\n\nYour client %s %s #%s <%s> with staff %s is running low in credit.\n\nPlease check account and communicate with your client.' % (csro_fname, doc['client_fname'], doc['client_lname'], doc['client_id'], doc['client_email'], staff_names)
        to = ['devs@remotestaff.com.au', csro_email]
        subject = 'Client Low On Credit'

        #send email via mailbox
        db = s['mailbox']
        now = get_ph_time()
        created = [now.year, now.month, now.day, now.hour, now.minute, now.second]
        doc = dict(
            sent = False,
            created = created,
            generated_by = 'celery PrepaidCSROAutoResponders.ClientRunningLowOnCredit',
            text = email_message,
            subject = subject,
            to = to,
        )
        doc['from'] = 'noreply@remotestaff.com.au'
        db.save(doc)


def run_tests():
    """
##~    >>> a = FirstMonthInvoicePayment('7bd42e55bd13ecb551ab00ed02c63171')

##~    >>> a = FirstMonthInvoicePayment('7bd42e55bd13ecb551ab00ed02c63171')

##~    >>> a = FirstMonthInvoicePayment('4b8f1a9904bd1251aa413485e40c6ecb')

##~    >>> a = FirstMonthInvoicePayment('652c3010d1dcbcf48b1f14ff2716bf02')

##~    >>> low_on_credit = ClientRunningLowOnCredit('30c7e2249927082e04558fcb844e149e')

##~    >>> low_on_credit = ClientRunningLowOnCredit('FAIL_DOC_ID')
##~    >>> low_on_credit = ClientRunningLowOnCredit('7bd42e55bd13ecb551ab00ed02c63171')
##~    >>> low_on_credit = ClientRunningLowOnCredit('4b8f1a9904bd1251aa413485e40c6ecb')
##~    >>> low_on_credit = ClientRunningLowOnCredit('652c3010d1dcbcf48b1f14ff2716bf02')
    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
