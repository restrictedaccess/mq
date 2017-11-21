#   2014-03-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated commission status to finished
#   2014-03-18  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   exported process_doc as a task and considered items with commission_id
#   -   remove the celery schedule and trigger the celery task process_doc when an invoice is set to paid
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-10-17 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import couchdb
from celery.task import task
from datetime import datetime, timedelta
from pytz import timezone
from decimal import Decimal

from persistent_mysql_connection import engine
from sqlalchemy.sql import text
from pprint import pprint

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

s = couchdb.Server(settings.COUCH_DSN)
db = s['client_docs']


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


def process_item(doc, item, currency, apply_gst):
    """returns a document id for invoice reference
    """

    now = get_ph_time(as_array = True)
    charge = Decimal(item['amount'])
    particular = item['description']
    if apply_gst == 'Y':
        charge += charge * Decimal('0.1')
        particular += ' plus GST'

    #round off
    charge = charge.quantize(Decimal('0.01'), rounding='ROUND_HALF_UP')

    #get running balance
    r = db.view('client/running_balance', key=doc['client_id'])

    if len(r.rows) == 0:
        running_balance = Decimal('0.0')
    else:                                                               
        running_balance = Decimal('%s' % r.rows[0].value)

    running_balance -= charge

    doc_transaction = dict(
        added_by = 'celery process paid invoice items',
        added_on = now,
        charge = '%0.2f' % charge,
        client_id = doc['client_id'],                                          
        credit = '0.00',
        credit_type = item['item_type'],
        currency = currency,
        remarks = 'Generated from paid invoice %s' % (doc['order_id']),
        type = 'credit accounting',
        running_balance = '%0.2f' % running_balance,
        particular = particular,
        invoice_ref = doc['_id'],
    )

    db.save(doc_transaction)
    return doc_transaction['_id']


@task(ignore_result=True)
def process_doc(doc_id):
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to process invoice items. Cannot find client_docs doc_id %s' % doc_id)

    if doc.has_key('items') == False:
        raise Exception('Failed to process invoice items. Cannot find items on doc_id %s' % doc_id)

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    client_id = doc['client_id']
    r = db.view('client/settings', startkey=[client_id, now],
        endkey=[client_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1)

    if len(r.rows) == 0:    #no client settings, alert devs
        raise Exception ('Failed to process invoice items %s. No settings found for %s' % (doc_id, client_id))

    currency, apply_gst = r.rows[0]['value']
    data = r.rows[0]
    client_setting_doc = data.doc
    client_setting_doc_id = data.id #db.get(client_setting_doc["_id"])
    

    #loop over items
    history = []
    for item in doc['items']:
        if item.has_key('item_type'):
            
            if item["item_type"] not in ["Regular Rostered Hours", "Currency Adjustment", "Adjustment Credit Memo", "Adjustment Over Time Work", "Over Payment", "Final Invoice"]:
                transaction_doc_id = process_item(doc, item, currency, apply_gst)
                changes = "charged item:%s amount:%s ref:%s" % (item['description'], item['amount'], transaction_doc_id)
                history.append(
                    dict(
                        timestamp = get_ph_time().strftime('%F %H:%M:%S'),
                        changes = changes,
                        by = 'celery process_order_items'
                    )
                )

        #process commission_id
        if item.has_key('commission_id'):
            commission_id = item.get('commission_id')
            if commission_id != '':
                conn = engine.connect()
                sql = text("""
                    UPDATE commission
                    SET status = 'finished',
                    payment_status = 'paid by client',
                    paid_by_client = 'y',
                    date_paid_by_client = :date_paid_by_client
                    WHERE commission_id = :commission_id
                """)
                date_paid_by_client = get_ph_time(as_array = False)
                conn.execute(sql, date_paid_by_client=date_paid_by_client, commission_id=commission_id)
                conn.close()

                # add history
                history.append(
                    dict(
                        timestamp = get_ph_time().strftime('%F %H:%M:%S'),
                        changes = 'processed commission_id %s' % commission_id,
                        by = 'celery process_order_items'
                    )
                )


    if len(history) == 0:
        return

    #retrieved once more for latest version
    doc = db.get(doc_id)

    #get history
    if doc.has_key('history') == False:
        old_history = []
    else:
        old_history = doc['history']

    doc['history'] = old_history + history
    doc['items_charged'] = 'Y'
    doc['mongo_synced'] = False	
    db.save(doc)
    
    #Check the Client setting
    #For New prepaid clients. Update days_before_suspension to 0 once invoice is paid
    client_doc = db.get(client_setting_doc_id)
    if client_doc["days_before_suspension"] != -30:
        if client_doc.has_key('new_client'):
            if client_doc['new_client'] == True:
                client_doc['new_client'] = False
                client_doc['days_before_suspension'] = 0
                client_doc['mongo_synced'] = False    
                db.save(client_doc)
    return


if __name__ == '__main__':
    process_doc('FAIL TEST')
    process_doc('e7d684680dc97c5fa6886a8497226bfd')
