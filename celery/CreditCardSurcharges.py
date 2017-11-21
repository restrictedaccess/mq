#   2013-10-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task to charge NAB transactions
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed sqlalchemy imports
#   2012-05-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a function to query the assigned csro email
#   -   send an email alert if the assigned csro has been removed

from celery.task import task, Task
from celery.execute import send_task
import string
import couchdb
from decimal import Decimal
from pprint import pprint, pformat
from Cheetah.Template import Template

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

from datetime import datetime, date, timedelta
import pytz
from pytz import timezone
import calendar

import re

import settings 
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

couch_server = couchdb.Server(settings.COUCH_DSN)
db_client_docs = couch_server['client_docs']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')


class ClientDocumentError(Exception):
    def __init__(self, value):
        self.value = value
        logging.info('error %s' % self.value)
    def __str__(self):
        return repr(self.value)


#@task(ignore_result=True)
class CreateSurcharges(Task):
    def run(self, doc_id):
        """given the doc_id, create surcharges
        check workflow 2537
        """
        logging.info('checking document %s ' % doc_id)
        couch_server = couchdb.Server(settings.COUCH_DSN)
        db_client_docs = couch_server['client_docs']
        doc = db_client_docs.get(doc_id)

        if doc == None:
            raise ClientDocumentError('document %s not found for creating surcharges' % doc_id)

        if doc.has_key('type') == False:
            raise ClientDocumentError('%s doc has no type field' % doc_id)

        if doc['type'] != 'secure_pay_transaction':
            raise ClientDocumentError('%s is not a secure_pay_transaction type doc' % doc_id)

        if doc.has_key('approved') == False:
            raise ClientDocumentError('%s doc has no approved field' % doc_id)

        if doc['approved'] != 'Yes':
            raise ClientDocumentError('%s document is not approved' % doc_id)

        if doc.has_key('cardtype') == False:
            raise ClientDocumentError('%s doc has no cardtype field' % doc_id)

        if doc.has_key('amount') == False:
            raise ClientDocumentError('%s doc has no amount field' % doc_id)

        if doc.has_key('currency') == False:
            raise ClientDocumentError('%s doc has no currency field' % doc_id)

        if doc.has_key('client_id') == False:
            raise ClientDocumentError('%s doc has no client_id field' % doc_id)
        else:
            leads_id = int(doc['client_id'])

        if doc.has_key('carddescription') == False:
            carddescription = 'Unknown'
        else:
            carddescription = doc['carddescription']

        currency = doc['currency']
        if currency in ['USD','GBP']:
            merchant_facility_percentage = 2
        else:
            cardtype = doc['cardtype']
            if cardtype in [5, 6]:  #mastercard or visa, based on secure pay documentation Secure_XML_API_Integration_Guide.pdf
                merchant_facility_percentage = 1
            else:
                merchant_facility_percentage = 2

        now = self.__get_phil_time__(as_array = True)

        amount = Decimal(doc['amount'])
        charge = amount * Decimal(merchant_facility_percentage) / Decimal(100)
        particular = '%s%% Merchant Facility Surcharge for the %s %0.2f %s Card Payment' % (merchant_facility_percentage, currency, amount, carddescription)

        r = db_client_docs.view('client/running_balance', key=leads_id)

        if len(r.rows) == 0:
            running_balance = Decimal('0')
        else:                                                               
            running_balance = Decimal('%s' % r.rows[0].value)

        running_balance -= charge

        doc_transaction = dict(
            added_by = 'automatic charge on payment (celery:CreditCardSurcharges.py)',
            added_on = now,
            charge = '%0.2f' % charge,
            client_id = leads_id,                                          
            credit = '0.00',
            credit_type = 'CARD_SURCHARGE',
            currency = currency,
            remarks = 'Credit Card Surcharge',
            type = 'credit accounting',
            running_balance = '%0.2f' % running_balance,
            particular = particular
        )

        db_client_docs.save(doc_transaction)
        return doc_transaction.copy()
##~        pprint(doc_transaction.copy())


    def __get_phil_time__(self, as_array = False):
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


@task(ignore_result=True)
def NAB_surcharge(doc_credit_accounting, doc_nab_transaction):
    """create surcharges for NAB payment
    """
    logging.info('doc_credit_accounting %s' % doc_credit_accounting)
    logging.info('doc_nab_transaction %s' % doc_nab_transaction)
    
    #get card type
    card_number = doc_nab_transaction.get('GET').get('pan')
    card_type = GetCardType(card_number)
    if card_type in ('Visa', 'MasterCard'):
        merchant_facility_percentage = 1
    elif card_type == 'American Express':
        merchant_facility_percentage = 2
    else:
        raise ClientDocumentError('CreateNABSurcharges : unknown card type for doc_nab_transaction %s , doc_credit_accounting %s' % (doc_nab_transaction, doc_credit_accounting))

    currency = doc_credit_accounting.get('currency')
    leads_id = doc_credit_accounting.get('client_id')
    now = GetPhTime(as_array = True)

    amount = Decimal(doc_credit_accounting.get('credit'))
    charge = amount * Decimal(merchant_facility_percentage) / Decimal(100)
    particular = '%s%% Merchant Facility Surcharge for the %s %0.2f %s Card Payment' % (merchant_facility_percentage, currency, amount, card_type)

    couch_server = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = couch_server['client_docs']

    r = db_client_docs.view('client/running_balance', key=leads_id)

    if len(r.rows) == 0:
        running_balance = Decimal('0')
    else:                                                               
        running_balance = Decimal('%s' % r.rows[0].value)

    running_balance -= charge

    doc_transaction = dict(
        added_by = 'automatic charge on payment (celery:CreditCardSurcharges.py)',
        added_on = now,
        charge = '%0.2f' % charge,
        client_id = leads_id,                                          
        credit = '0.00',
        credit_type = 'CARD_SURCHARGE',
        currency = currency,
        remarks = 'Credit Card Surcharge',
        type = 'credit accounting',
        running_balance = '%0.2f' % running_balance,
        particular = particular
    )

    db_client_docs.save(doc_transaction)
    send_task('notify_devs.send', ['NAB Credit Card Surcharge FEE EXTRACTED', 'Please check NAB payment doc %s, fee:%s' % (doc_nab_transaction, charge)])  #TODO delete this notification once stable


def GetPhTime(as_array = False):
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



def GetCardType(card_number):
    """Given the card_number (pan result from NAB payment gateway),
    return card type
    based on http://en.wikipedia.org/wiki/Bank_card_number
    no checking of length and validation, just the IIN ranges
    Check only for American Express, Visa and MasterCard

    >>> GetCardType('376001...000')
    'American Express'
    >>> GetCardType('407220...920')
    'Visa'
    >>> GetCardType('419973...118')
    'Visa'
    >>> GetCardType('450949...615')
    'Visa'
    >>> GetCardType('494052...096')
    'Visa'
    >>> GetCardType('514045...948')
    'MasterCard'
    >>> GetCardType('521729...189')
    'MasterCard'
    >>> GetCardType('558850...774')
    'MasterCard'

    """
    if re.match('^3[47]', card_number):
        return 'American Express'

    elif re.match('^4[0-9]', card_number):
        return 'Visa'

    elif re.match('^5[1-5]', card_number):
        return 'MasterCard'


def run_tests():
    """
##~    >>> charges = CreateSurcharges()
##~    >>> charges.run('test_fail')
##~    >>> charges.run('008c6ec20adda363889d1ff6340019a9')
##~    >>> charges.run('d01f9a68ebbd8771f1364dd95bf2bf9c')
##~    >>> charges.run('6b3ead2fafe5fbf7548a5b36a93a1baf')

    >>> s = couchdb.Server(settings.COUCH_DSN)
    >>> db = s['client_docs']
    >>> doc_nab_transaction = db.get('b2eaa7b0b894d691b405f70bfb23a8ad').copy()
    >>> doc_credit_accounting = db.get('f145494e2bbe234ac93a4d9c3f0b88c9').copy()
    >>> NAB_surcharge(doc_credit_accounting, doc_nab_transaction)

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
