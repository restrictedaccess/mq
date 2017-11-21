#   2013-10-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   prevent adding item if start_date of invoice is the same as the date of the rate change
#   2013-08-16  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   Task #4486, computation of pay before date should count number of days before "0"
#   2013-07-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add redis lock to prevent creating invoices at the same time
#   -   used the latest id from subcontractors_scheduled_close_cotract
#   2013-05-14  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix breakout dates on client rate changes
#   2013-05-13  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used subcontractors_scheduled_client_rate for checking scheduled rate change
#   -   fix float to decimal conversion
#   -   fix on adding week days
#   -   zero out hours/minutes/seconds on start_date reference
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   wf 3666, Adjust Pay Before date
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   do not include staff if scheduled termination date is earlier than clients expected 0 balance
#   2013-01-16 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   anticipate all items having termination date
#   2013-01-08 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated task to accommodate creation of invoice from finish work celery task
#   -   check if invoice has only one item and has termination date
#   2012-12-17 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered subcontractors_scheduled_close_cotract.scheduled_date
#   -   notify devs for monitoring updated total_hours
#   2012-12-11 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   just used the fixed 4/8 hours per day
#   2012-12-10 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add an override on number of hours to be invoiced, wf 3376 (Christina Weber # 6630)
#   2012-10-01 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task for creating invoice

import settings
import couchdb
import re
import string
from pprint import pformat, pprint
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
from email.mime.image import MIMEImage

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

from sqlalchemy.sql import text
from persistent_mysql_connection import engine
import certifi
import redis

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import certifi

utc = timezone('UTC')
phtz = timezone('Asia/Manila')

WORKING_WEEKDAYS = 22

VERIFY_INVOICE_MESSAGE = """
Hi Accounts,

Please verify invoice %s. We have a scheduled client rate change on one of his staff.

Related docs are:

doc_order : %s

rate_changes : %s
"""


def get_ph_time(as_array=False):
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    if as_array == True:
        return [now.year, now.month, now.day, now.hour, now.minute, now.second]
    else:
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


def add_week_days(reference_date, days):
    """add week days given the reference_date
    """
    #step a day back to include the reference date
    return_date = reference_date - timedelta(days = 1)

    #start looping
    i = 0
    while i < days:
        return_date = return_date + timedelta(days = 1)
        if return_date.strftime('%a') in ['Sat', 'Sun']:
            continue
        i += 1

    if return_date.strftime('%a') == 'Sat':
        return_date = return_date + timedelta(days = 2)

    if return_date.strftime('%a') == 'Sun':
        return_date = return_date + timedelta(days = 1)

    return return_date


def get_week_days(reference_date, future_date):
    """given two dates, return number of days
    """
    #check dates
    if reference_date > future_date:
        raise Exception('reference_date > future_date', 'reference_date:%s future_date:%s' % (reference_date, future_date))

    i = 0
    while reference_date < future_date:
        if reference_date.strftime('%a') not in ['Sat', 'Sun']:
            i += 1

        reference_date += timedelta(days = 1)

    return i


def notify_on_rate_changes(doc_order, rate_changes):
    """save document to couchdb mailbox
    >>> s = couchdb.Server(settings.COUCH_DSN)
    >>> db = s['client_docs']
    >>> doc_order = db.get('086150aa5571f922e79b222abc392b76')
    >>> rate_changes = {'lname': u'Cipriano', 'rate': Decimal('615.85'), 'work_status': u'Part-Time', 'fname': u'Maria', 'subcontractors_id': 3598L, 'id': 3067L, 'start_date': datetime(2013, 4, 19, 0, 0)}
    >>> notify_on_rate_changes(doc_order.copy(), rate_changes)
    """

    text_message = VERIFY_INVOICE_MESSAGE % (doc_order['order_id'], pformat(doc_order, 4), pformat(rate_changes, 4))

    doc = dict(
        to = ['accounts@remotestaff.com.au'],
        bcc = ['devs@remotestaff.com.au'],
        created = get_ph_time(as_array=True),
        generated_by = 'celery task prepaid_create_invoice.notify_on_rate_changes',
        text = text_message,
        subject = 'Invoice Created With Scheduled Rate Changes, Invoice:%s' % doc_order['order_id'],
        sent = False,
    )
    doc['from'] = "Celery Process On Staff Finish Work<noreply@remotestaff.com.au>"

    s = couchdb.Server(settings.COUCH_DSN)
    db = s['mailbox']
    db.save(doc)


def check_for_latest_rate_change_by_subcon_id(subcontractors_id, start_date_time, end_date_time):
    """
    Based on assumption that there would only be one change in between
    start_date_time and end_date_time.
    More than one result, must notify devs and should return the latest record
    >>> check_for_latest_rate_change_by_subcon_id(2570, datetime(2013,5,1,0,0,0), datetime(2013,6,1,0,0,0))
    {'lname': u'Bautista', 'rate': Decimal('2097.31'), 'work_status': u'Full-Time', 'fname': u'Anthony James', 'subcontractors_id': 2570L, 'id': 11L, 'start_date': datetime.datetime(2013, 5, 14, 0, 0)}

    >>> check_for_latest_rate_change_by_subcon_id(3000, datetime(2013,4,5,0,0,0), datetime(2013,5,5,0,0,0)) == None
    True


    >>> check_for_latest_rate_change_by_subcon_id(3520, datetime(2013,4,5,0,0,0), datetime(2013,5,5,0,0,0)) == None
    True

    """

    changes = []
    conn = engine.connect()
    sql = text("""
        SELECT id, subcontractors_id, scheduled_date, rate, work_status, status, added_by_id, date_added
        FROM subcontractors_scheduled_client_rate 
        WHERE subcontractors_id = :subcontractors_id
        AND (scheduled_date BETWEEN :start_date_time AND :end_date_time)
        AND status = 'waiting'
        """)
    data_client_rate = conn.execute(sql, 
        subcontractors_id=subcontractors_id, 
        start_date_time=start_date_time, 
        end_date_time=end_date_time)

    sql = text("""
        SELECT p.fname, p.lname
        FROM subcontractors AS s
        JOIN personal AS p
        ON s.userid = p.userid
        WHERE s.id = :subcontractors_id
    """)
    d = conn.execute(sql, subcontractors_id = subcontractors_id).fetchone()

    for a in data_client_rate:
        changes.append(dict(
            id = a.id,
            subcontractors_id = a.subcontractors_id,
            work_status = a.work_status,
            fname = d.fname,
            lname = d.lname,
            start_date = a.scheduled_date,
            rate = Decimal('%0.2f' % a.rate)
            )
        )

    conn.close()
    if len(changes) > 1:
        send_task('notify_devs.send', ['MORE THAN ONE subcontractors_client_rate record found!', 'Please check records from subcontractors_client_rate:\n\n%s' % (pformat(changes, 4))])
        change = [-1]
    elif len(changes) == 1:
        change = changes[0]
    else:
        change = None

    return change
        

@task
def create_invoice(leads_id, working_days, deduct_running_balance, added_by):
    """generates a couchdb document and returns a copy
    working_days = WORKING_WEEKDAYS 
    deduct_running_balance = flag to indicate if we need to add the running balance
    added_by = to document/track the creator of order/invoice, 'celery prepaid_create_invoice'
    """

    #check locks using redis
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_key = 'create_invoice_lock:%s' % leads_id
    lock = redis_client.setnx(redis_key, leads_id)
    if lock == False:
        raise Exception('FAILED to acquire lock for %s' % redis_key, 'FAILED to acquire lock for %s' % redis_key)

    #add expire in case creation of invoice fails
    redis_client.expire(redis_key, 5)

    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    data = r.rows[0]
    doc = data.doc
    couch_currency, apply_gst = data['value']

    #get items
    sql = text("""SELECT s.id, s.client_price, s.currency,
        s.job_designation, s.work_status, s.starting_date,
        p.fname, p.lname
        FROM subcontractors as s
        LEFT JOIN personal AS p
        ON s.userid = p.userid
        WHERE s.leads_id = :leads_id
        AND prepaid='yes'
        AND s.status in ('ACTIVE', 'suspended')
        """)
    conn = engine.connect()
    items = conn.execute(sql, leads_id = leads_id).fetchall()

    sub_total = Decimal(0)
    invoice_items = []
    currency_check = []

    #check clients running balance
    r = db_client_docs.view('client/running_balance', key=leads_id)
    
    if len(r.rows) == 0:
        running_balance = Decimal(0)
    else:
        running_balance = Decimal('%0.2f' % r.rows[0].value)

    if len(items) == 0:
        logging.info('No items found for leads_id %s' % leads_id)
    
    import ClientsWithPrepaidAccounts
    clients_daily_rate = ClientsWithPrepaidAccounts.get_clients_daily_rate(leads_id)
    #r = send_task("ClientsWithPrepaidAccounts.get_clients_daily_rate", [leads_id,])
    #clients_daily_rate = r.get()

    #given the running_balance and clients_daily_rate, get possible number of days for the start_date
    if clients_daily_rate == 0:
        max_days = 0
    else:
        max_days = int(running_balance / clients_daily_rate)
        if max_days < 0:
            max_days = 0

    start_date = add_week_days(get_ph_time(), max_days)
    #zero out hours/minutes/seconds
    start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
    end_date = add_week_days(start_date, working_days)

    history = []
    count_item_modified = 0
    i = 1
    rate_changes = []


    #load currency adjustment module
    import currency_adjustment

    for item in items:
        sid, client_price, currency, job_designation, work_status, starting_date, fname, lname = item
        
        #convert starting_date to datetime format, currently its on date format
        starting_date = datetime(starting_date.year, starting_date.month, starting_date.day, 0, 0, 0)

        #skip item if starting_date hasn't started yet
        now_date = get_ph_time(as_array=False)
        if starting_date > now_date:
            send_task('skype_messaging.notify_skype_id', ['skipped invoice item since it has not started yet \n: %s' % pformat(item.items(), 4), 'locsunglao'])     #TODO remove once stable
            continue

        fname = string.capwords(string.strip(fname))
        lname = string.capwords(string.strip(lname))

        if couch_currency != currency:
            raise Exception('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))

        if work_status == 'Part-Time':
            hours_per_day = 4
        else:
            hours_per_day = 8

        total_hours = Decimal('%0.2f' % (working_days * hours_per_day))
        staff_hourly_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0 / hours_per_day))

        override_hours_per_invoice = None
        if doc.has_key('override_hours_per_invoice'):
            if doc['override_hours_per_invoice'].has_key('%s' % sid):
                total_hours = doc['override_hours_per_invoice']['%s' % sid]['total_hours']
                override_hours_per_invoice = Decimal('%s' % total_hours)

        #check if contract is scheduled for termination
        sql = text("""
            SELECT id, scheduled_date
            FROM subcontractors_scheduled_close_cotract
            WHERE status = 'waiting'
            AND subcontractors_id = :sid
            ORDER BY id DESC
        """)
        scheduled = conn.execute(sql, sid = sid).fetchall()
        
        #notify devs if more than one record found!
        if len(scheduled) > 1:
            send_task('notify_devs.send', ['MORE THAN ONE subcontractors_scheduled_close_cotract record found!', 'Please check subcontractors %s:\n\n%r' % (sid, scheduled)])

        total_hours_before_termination = Decimal('0')
        if len(scheduled) > 0:  #get number of days
            week_days = get_week_days(get_ph_time(), scheduled[0].scheduled_date)
            total_hours_before_termination = Decimal('%0.2f' % (week_days * hours_per_day))

            end_date_item = scheduled[0].scheduled_date

            if total_hours > total_hours_before_termination:
                total_hours = total_hours_before_termination
                count_item_modified += 1
            
            #check if end_date_item is earlier than start_date
            if end_date_item <= start_date:
                changes = 'skipped inclusion of staff %s %s [%s] due to termination date at %s' % (fname, lname, job_designation, end_date_item)
                history.append(dict(
                    by = 'celery process prepaid_create_invoice.py',
                    timestamp = get_ph_time().strftime('%F %H:%M:%S'),
                    changes = changes
                    )
                )
                send_task('notify_devs.send', ['INVOICE CREATION NOTICE, SKIPPED INCLUSION OF STAFF DUE TO TERMINATION DATE', changes])
                continue

        else:
            end_date_item = end_date

        #check if there is a change on client rate
        rate_change = check_for_latest_rate_change_by_subcon_id(sid, start_date, end_date_item)
        
        #store items for proper currency adjustment allocation
        currency_adjustment_list = []
        
        if rate_change != None:
            #need to add two items
            #one from start_date to rate_change['start_date'] -1 day
            #one from rate_change['start_date'] to end_date_item

            rate_changes.append(rate_change)    #needed to flag accounts/admin later

            #add first item
            start_date_rate_change = rate_change['start_date']
            end_date_before_change = start_date_rate_change - timedelta(days = 1)

            #consider weekends
            if start_date_rate_change.strftime('%a') == 'Sat':
                start_date_rate_change = start_date_rate_change + timedelta(days=2)
                end_date_before_change = start_date_rate_change - timedelta(days=3)
            
            if start_date_rate_change.strftime('%a') == 'Sun':
                start_date_rate_change = start_date_rate_change + timedelta(days=1)
                end_date_before_change = start_date_rate_change - timedelta(days=3)

            if start_date_rate_change.strftime('%a') == 'Mon':
                end_date_before_change = start_date_rate_change - timedelta(days=3)

            #check if invoice start date is the same as the date when the rate changes
            if start_date < end_date_before_change:
                week_days_before_rate_change = get_week_days(start_date, end_date_before_change) + 1

                if override_hours_per_invoice != None:
                    send_task('notify_devs.send', ['override_hours_per_invoice and rate_change conflict', 'Please Check %s\nwith override_hours_per_invoice %s' % (pformat(rate_change, 4), override_hours_per_invoice)])

                #get rates via celery task
                r = send_task('subcontractors.get_rate_work_status_hours_per_day', [sid, start_date])
                data_before_change = r.get()

                total_hours = Decimal('%0.2f' % (week_days_before_rate_change * data_before_change['hours_per_day']))
                staff_hourly_rate = data_before_change['hourly_rate']
                amount = Decimal(total_hours * staff_hourly_rate)

                invoice_item = dict(
                    item_id = i,
                    start_date = [start_date.year, start_date.month, start_date.day],
                    end_date = [end_date_before_change.year, end_date_before_change.month, end_date_before_change.day],
                    unit_price = '%0.2f' % staff_hourly_rate,
                    qty = '%0.2f' % total_hours,
                    amount = '%0.2f' % amount,
                    description = '%s %s [%s]' % (fname, lname, job_designation),
                    item_note = 'before rate_change : %s' % rate_change,
                    subcon_id = sid,
                    staff_name = "%s %s" % (fname, lname),
                    job_designation = "%s" % job_designation,
                    item_type = "Regular Rostered Hours"
                    )

                invoice_items.append(invoice_item)
                sub_total += amount
                i += 1
                

            #add the second item
            week_days_after_rate_change = get_week_days(start_date_rate_change, end_date_item) + 1

            #hours_per_day might have changed, re-evaluate
            if rate_change['work_status'] == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8

            #recompute staff_hourly_rate
            client_price_after_rate_change = rate_change['rate']
            staff_hourly_rate = client_price_after_rate_change * Decimal('12.0') / Decimal('52.0') / Decimal('5.0') / Decimal('%0.2f' % hours_per_day)

            total_hours = Decimal('%0.2f' % (week_days_after_rate_change * hours_per_day))
            amount = total_hours * staff_hourly_rate
            
            invoice_item = dict(
                item_id = i,
                start_date = [start_date_rate_change.year, start_date_rate_change.month, start_date_rate_change.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = '%s %s [%s] - %0.2fhrs@%0.2f/hr' % (fname, lname, job_designation, total_hours, staff_hourly_rate),
                item_note = 'after rate_change : %s' % rate_change,
                subcon_id = sid,
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Currency Adjustment"
                )

            invoice_items.append(invoice_item)
            sub_total += amount
            i += 1
            
            
            #currency adjustment items
            forex = currency_adjustment.get_forex_rate_per_staff(sid)
            subcon = currency_adjustment.get_contract_detail(sid)
            currency_difference = currency_adjustment.get_currency_adjustment_per_staff(sid)
            currency_difference_peso = currency_adjustment.get_currency_adjustment_peso_per_staff(sid)
            
            couch_currency, couch_apply_gst = currency_adjustment.get_currency_apply_gst_client(leads_id)
            forex_rate = float(forex.rate)
            subcon_current_rate = float(subcon.current_rate)
            
            amount = Decimal(currency_difference) * total_hours
            description = "Currency Adjustment (Contract Rate 1 %s = %.02f PESO vs. Current Rate 1 %s = %.02f PESO, Currency Difference of %.02f PESO for your staff %s [%s])" % (couch_currency, subcon_current_rate, couch_currency,forex_rate,currency_difference_peso,  fname+" "+lname, job_designation)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.4f' % currency_difference,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = description,
                subcon_id = sid,
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Currency Adjustment"
                )
            invoice_items.append(invoice_item)
            sub_total += amount
            
            currency_adjustment_list.append(dict(
                subcon_id = sid,
                currency_difference = currency_difference,
                start_date = start_date,
                end_date = end_date_item,
                total = float("{0:.2f}".format(amount))
            ))
            
            i+=1
            
        else:
            amount = Decimal(total_hours * staff_hourly_rate)            
            
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                subcon_id = sid,
                description = '%s %s [%s]' % (fname, lname, job_designation),
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Regular Rostered Hours"
                )

            if total_hours_before_termination != Decimal('0'):  #just add item_note
                invoice_item['item_note'] = 'scheduled_date:%s total_hours_before_termination:%0.2f' % (scheduled[0].scheduled_date, total_hours_before_termination)

            invoice_items.append(invoice_item)

            sub_total += amount

            i += 1
            
            #currency adjustment items
            forex = currency_adjustment.get_forex_rate_per_staff(sid)
            subcon = currency_adjustment.get_contract_detail(sid)
            currency_difference = currency_adjustment.get_currency_adjustment_per_staff(sid)
            currency_difference_peso = currency_adjustment.get_currency_adjustment_peso_per_staff(sid)
            
            couch_currency, couch_apply_gst = currency_adjustment.get_currency_apply_gst_client(leads_id)
            forex_rate = float(forex.rate)
            subcon_current_rate = float(subcon.current_rate)
            
            amount = Decimal(currency_difference) * total_hours
            description = "Currency Adjustment (Contract Rate 1 %s = %.02f PESO vs. Current Rate 1 %s = %.02f PESO, Currency Difference of %.02f PESO for your staff %s [%s])" % (couch_currency, subcon_current_rate, couch_currency,forex_rate,currency_difference_peso,  fname+" "+lname, job_designation)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.4f' % currency_difference,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = description,
                subcon_id = sid,
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Currency Adjustment"
                )
            invoice_items.append(invoice_item)
            
            currency_adjustment_list.append(dict(
                subcon_id = sid,
                currency_difference = currency_difference,
                start_date = start_date,
                end_date = end_date_item,
                total = float("{0:.2f}".format(amount))
            ))
            sub_total += amount
            i+=1

    if deduct_running_balance == True:
        if running_balance > Decimal(0):    #deduct running balance
            now_date = datetime(now[0], now[1], now[2])
            invoice_item = dict(
                item_id = i,
                unit_price = '%0.2f' % running_balance,
                qty = '-1.00',
                amount = '%0.2f' % (running_balance * Decimal('-1.00')),    #negative amount
                description = 'Credit From Available Balance as of %s' % now_date.strftime('%b %d, %Y')
            )

            invoice_items.append(invoice_item)
            sub_total += running_balance * Decimal('-1.00')

    #get fname, lname
    sql = text("""SELECT fname, lname, email, registered_domain from leads
        WHERE id = :leads_id
        """)
    client_fname, client_lname, client_email, registered_domain = conn.execute(sql, leads_id = leads_id).fetchone()

    gst_amount = Decimal(0)
    total_amount = sub_total
    if apply_gst == 'Y':
        gst_amount = sub_total * Decimal('0.1')
        total_amount = total_amount + gst_amount

    #get last order id
    r = db_client_docs.view('client/last_order_id', startkey=[leads_id, "%s-999999999" % leads_id],
        endkey=[leads_id, ""], descending=True, limit=1)

    if len(r.rows) == 0:
        last_order_id = 1
    else:
        last_order_id_str = r.rows[0].key[1]
        x, last_order_id = string.split(last_order_id_str, '-')
        last_order_id = int(last_order_id)
        last_order_id += 1

    order_id = '%s-%08d' % (leads_id, last_order_id)

    #days_weekdays = add_week_days(get_ph_time(), 5)
    #start_date_day_after = start_date + timedelta(days=days_weekdays)
    start_date_day_after = add_week_days(get_ph_time(), 5)

    pay_before_date = [
        start_date_day_after.year, 
        start_date_day_after.month, 
        start_date_day_after.day, 
        start_date_day_after.hour, 
        start_date_day_after.minute, 
        start_date_day_after.second, 
    ]

    doc_order = dict(
        added_by = added_by,
        apply_gst = apply_gst,
        client_id = leads_id,
        history = history,
        type = 'order',
        added_on = now,
        items = invoice_items,
        status = 'new',
        order_id = order_id,
        sub_total = '%0.2f' % sub_total,
        total_amount = '%0.2f' % total_amount,
        gst_amount = '%0.2f' % gst_amount,
        client_fname = client_fname,
        client_lname = client_lname,
        client_email = client_email,
        registered_domain = registered_domain, 
        currency = couch_currency,
        pay_before_date = pay_before_date,
    )

    doc_order['running_balance'] = '%0.2f' % running_balance

    if len(invoice_items) == 0:
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id %s %s' % (leads_id, doc_order))

    if len(invoice_items) == count_item_modified:
        raise Exception('Ignored creation of Prepaid Based Invoice', 'Invoice has same count of invoice_items and count_item_modified, probably scheduled for termination\n%s' % (pformat(doc_order, 4)))

    db_client_docs.save(doc_order)


    #store currency adjustment in a block per subcon
    #for item in currency_adjustment_list:
        #currency_adjustment.save_currency_adjustment_allocation_per_staff(item["subcon_id"], item["start_date"], item["end_date"], order_id, item["currency_difference"], item["total"])
    logging.info('created prepaid invoice %s' % doc_order['_id'])

    if count_item_modified >= 1:
        send_task('notify_devs.send', ['count_item_modified', 'total hours modified for:\n\norder:%s doc_id:%s' % (order_id, doc_order['_id'])])   #TODO notification can be deleted once stable/verified

    conn.close()

    if len(rate_changes) != 0:  #notify accounts and devs
        notify_on_rate_changes(doc_order.copy(), rate_changes)

    #release redis lock
    redis_client.delete(redis_key)
    
    #invoke xero for invoicing
    sync_invoice_mongo(leads_id)
    return doc_order.copy()


@task
def create_first_month_invoice(leads_id, subcon_id):
    """generates a couchdb document and returns a copy
    working_days = WORKING_WEEKDAYS
    deduct_running_balance = flag to indicate if we need to add the running balance
    added_by = to document/track the creator of order/invoice, 'celery prepaid_create_invoice'
    """
    leads_id = int(leads_id)
    
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    data = r.rows[0]
    doc = data.doc
    couch_currency, apply_gst = data['value']
    #return couch_currency
    if doc.has_key('days_to_invoice'):
        working_days = doc['days_to_invoice']
    else:
        working_days = WORKING_WEEKDAYS 
    
    days_before_suspension=0
    
    if doc.has_key('days_before_suspension'):
        days_before_suspension = doc['days_before_suspension']

    #Prepaid clients only
    if days_before_suspension != -30:    
    
        deduct_running_balance = False
        added_by = "celery prepaid_create_invoice.create_first_month_invoice"
        #check locks using redis
        redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        redis_key = 'create_invoice_lock:%s' % leads_id
        lock = redis_client.setnx(redis_key, leads_id)
        if lock == False:
            raise Exception('FAILED to acquire lock for %s' % redis_key, 'FAILED to acquire lock for %s' % redis_key)

        #add expire in case creation of invoice fails
        redis_client.expire(redis_key, 5)
        
        #get items
        sql = text("""SELECT s.id, s.client_price, s.currency,
            s.job_designation, s.work_status, s.starting_date,
            p.fname, p.lname
            FROM subcontractors as s
            LEFT JOIN personal AS p
            ON s.userid = p.userid
            WHERE s.leads_id = :leads_id
            AND s.id = :subcon_id
            AND prepaid='yes'
            AND s.status in ('ACTIVE', 'suspended')
            """)
        conn = engine.connect()
        items = conn.execute(sql, leads_id = leads_id, subcon_id = subcon_id).fetchall()
    
        sub_total = Decimal(0)
        invoice_items = []
        currency_check = []
    
        #check clients running balance
        r = db_client_docs.view('client/running_balance', key=leads_id)
        
        if len(r.rows) == 0:
            running_balance = Decimal(0)
        else:
            running_balance = Decimal('%0.2f' % r.rows[0].value)
    
        if len(items) == 0:
            logging.info('No items found for leads_id %s' % leads_id)
        
        import ClientsWithPrepaidAccounts
        clients_daily_rate = ClientsWithPrepaidAccounts.get_clients_daily_rate(leads_id)
        #r = send_task("ClientsWithPrepaidAccounts.get_clients_daily_rate", [leads_id,])
        #clients_daily_rate = r.get()
    
        #given the running_balance and clients_daily_rate, get possible number of days for the start_date
        if clients_daily_rate == 0:
            max_days = 0
        else:
            max_days = int(running_balance / clients_daily_rate)
            if max_days < 0:
                max_days = 0
    
        #start_date = add_week_days(get_ph_time(), max_days)
        #zero out hours/minutes/seconds
        #start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
        #end_date = add_week_days(start_date, working_days)
    
        history = []
        i = 1
        rate_changes = []
    
        #load currency adjustment module
        import currency_adjustment
        
        #store items for proper currency adjustment allocation
        currency_adjustment_list = []
        
        for item in items:
            sid, client_price, currency, job_designation, work_status, starting_date, fname, lname = item
            
            #convert starting_date to datetime format, currently its on date format
            starting_date = datetime(starting_date.year, starting_date.month, starting_date.day, 0, 0, 0)
            start_date = starting_date
            #start_date = add_week_days(starting_date, max_days)
            
            #zero out hours/minutes/seconds
            #start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
            end_date = add_week_days(start_date, working_days)
    
            #skip item if starting_date hasn't started yet
            now_date = get_ph_time(as_array=False)
            #if starting_date > now_date:
            #    send_task('skype_messaging.notify_skype_id', ['skipped invoice item since it has not started yet \n: %s' % pformat(item.items(), 4), 'locsunglao'])     #TODO remove once stable
            #    continue
    
            fname = string.capwords(string.strip(fname))
            lname = string.capwords(string.strip(lname))
    
            if couch_currency != currency:
                raise Exception('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))
    
            if work_status == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8
    
            total_hours = Decimal('%0.2f' % (working_days * hours_per_day))
            staff_hourly_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0 / hours_per_day))
            override_hours_per_invoice = Decimal('%s' % total_hours)
        
        
            end_date_item = end_date
    
            amount = Decimal(total_hours * staff_hourly_rate)            
            
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                subcon_id = sid,
                description = '%s %s [%s]' % (fname, lname, job_designation),
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Regular Rostered Hours"
                )
    
    
            invoice_items.append(invoice_item)
    
            sub_total += amount
    
            i += 1
            
            #currency adjustment items
            forex = currency_adjustment.get_forex_rate_per_staff(sid)
            subcon = currency_adjustment.get_contract_detail(sid)
            currency_difference = currency_adjustment.get_currency_adjustment_per_staff(sid)
            currency_difference_peso = currency_adjustment.get_currency_adjustment_peso_per_staff(sid)
            
            couch_currency, couch_apply_gst = currency_adjustment.get_currency_apply_gst_client(leads_id)
            forex_rate = float(forex.rate)
            subcon_current_rate = float(subcon.current_rate)
            
            amount = Decimal(currency_difference) * total_hours
            description = "Currency Adjustment (Contract Rate 1 %s = %.02f PESO vs. Current Rate 1 %s = %.02f PESO, Currency Difference of %.02f PESO for your staff %s [%s])" % (couch_currency, subcon_current_rate, couch_currency,forex_rate,currency_difference_peso,  fname+" "+lname, job_designation)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.4f' % currency_difference,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = description,
                subcon_id = sid,
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                item_type = "Currency Adjustment"
                )
            invoice_items.append(invoice_item)
            sub_total += amount
            
            currency_adjustment_list.append(dict(
                subcon_id = sid,
                currency_difference = currency_difference,
                start_date = start_date,
                end_date = end_date_item,
                total = float("{0:.2f}".format(amount))
            ))
            
            i+=1
    
        if deduct_running_balance == True:
            if running_balance > Decimal(0):    #deduct running balance
                now_date = datetime(now[0], now[1], now[2])
                invoice_item = dict(
                    item_id = i,
                    unit_price = '%0.2f' % running_balance,
                    qty = '-1.00',
                    amount = '%0.2f' % (running_balance * Decimal('-1.00')),    #negative amount
                    description = 'Credit From Available Balance as of %s' % now_date.strftime('%b %d, %Y')
                )
    
                invoice_items.append(invoice_item)
                sub_total += running_balance * Decimal('-1.00')
    
        #get fname, lname
        sql = text("""SELECT fname, lname, email, registered_domain from leads
            WHERE id = :leads_id
            """)
        client_fname, client_lname, client_email, registered_domain = conn.execute(sql, leads_id = leads_id).fetchone()
    
        gst_amount = Decimal(0)
        total_amount = sub_total
        if apply_gst == 'Y':
            gst_amount = sub_total * Decimal('0.1')
            total_amount = total_amount + gst_amount
    
        #get last order id
        r = db_client_docs.view('client/last_order_id', startkey=[leads_id, "%s-999999999" % leads_id],
            endkey=[leads_id, ""], descending=True, limit=1)
    
        if len(r.rows) == 0:
            last_order_id = 1
        else:
            last_order_id_str = r.rows[0].key[1]
            x, last_order_id = string.split(last_order_id_str, '-')
            last_order_id = int(last_order_id)
            last_order_id += 1
    
        order_id = '%s-%08d' % (leads_id, last_order_id)
    
        start_date_day_after = get_ph_time() + timedelta(days=4)
    
        pay_before_date = [
            start_date_day_after.year, 
            start_date_day_after.month, 
            start_date_day_after.day, 
            start_date_day_after.hour, 
            start_date_day_after.minute, 
            start_date_day_after.second, 
        ]
    
        doc_order = dict(
            added_by = added_by,
            apply_gst = apply_gst,
            client_id = leads_id,
            history = history,
            type = 'order',
            added_on = now,
            items = invoice_items,
            status = 'new',
            order_id = order_id,
            sub_total = '%0.2f' % sub_total,
            total_amount = '%0.2f' % total_amount,
            gst_amount = '%0.2f' % gst_amount,
            client_fname = client_fname,
            client_lname = client_lname,
            client_email = client_email,
            registered_domain = registered_domain, 
            currency = currency,
            pay_before_date = pay_before_date,
        )
    
        doc_order['running_balance'] = '%0.2f' % running_balance
    
        if len(invoice_items) == 0:
            raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id %s %s' % (leads_id, doc_order))
    
        db_client_docs.save(doc_order)
        logging.info('created first month invoice %s' % doc_order['_id'])
    
        
        logging.info('sending first month invoice %s' % doc_order['_id'])
        send_email(doc_order['_id'])
        
        
        logging.info("sending email notification to accounts")
        
        logging.info('sending email notification for refund')
        
        message = "<p>Hi Team,</p>"
        message += "<p>&nbsp;</p>"
        message += "<p>First month invoice has been created for client %s %s.</p>" % (client_fname, client_lname)
        message += "<p>&nbsp;</p>"
        message += "<p>Please check Invoice Number %s.</p>" % order_id
        message += "<p>&nbsp;</p>"
        message += "<p>This is system generated. Please do not reply to this email.</p>"
        message += "<p>&nbsp;</p>"
        message += "<p>RS System</p>"
        
        doc = dict(
            to = ['accounts@remotestaff.com.au'],
            bcc = ['devs@remotestaff.com.au'],
            created = get_ph_time(as_array=True),
            generated_by = 'celery task prepaid_create_invoice.create_first_month_invoice',
            text = None,
            html = message,
            subject = 'First Month Invoice for Client  %s %s' % (client_fname, client_lname),
            sent = False,
        )
        doc['from'] = "No Reply<noreply@remotestaff.com.au>"
    
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['mailbox']
        db.save(doc)
            
            
        conn.close()


        #release redis lock
        redis_client.delete(redis_key)
    
        #invoke xero for invoicing
        sync_invoice_mongo(leads_id)
        return doc_order.copy()


@task
def create_client_final_invoice(final_invoice_id, subcon_id):

    #get subcon details. this will be used in invoice itens
    sql = text("""SELECT s.id, s.client_price, s.currency,
        s.job_designation, s.work_status, s.starting_date, s.leads_id,
        p.fname, p.lname
        FROM subcontractors as s
        LEFT JOIN personal AS p ON s.userid = p.userid
        WHERE s.id = :subcon_id
        """)
    conn = engine.connect()
    subcon = conn.execute(sql, subcon_id = subcon_id).fetchall()
    subcon = subcon[0]
    
    
    #get final invoice start and end date
    sql = text("""SELECT s.status, s.pay_hrs_to_staff_start_date, s.pay_hrs_to_staff_end_date
        FROM subcontractors_final_invoice as s
        WHERE s.id = :final_invoice_id
        """)
    conn = engine.connect()
    final_invoice = conn.execute(sql, final_invoice_id = final_invoice_id).fetchall()
    final_invoice = final_invoice[0]
    
    
    
    leads_id = int(subcon.leads_id)
    
    deduct_running_balance = False
    added_by = "celery prepaid_create_invoice.create_client_final_invoice"
    #check locks using redis
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_key = 'create_invoice_lock:%s' % leads_id
    lock = redis_client.setnx(redis_key, leads_id)
    if lock == False:
        raise Exception('FAILED to acquire lock for %s' % redis_key, 'FAILED to acquire lock for %s' % redis_key)

    #add expire in case creation of invoice fails
    redis_client.expire(redis_key, 5)
    
    

    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    data = r.rows[0]
    doc = data.doc
    couch_currency, apply_gst = data['value']
    
    
    if couch_currency != subcon.currency:
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))


    #check clients running balance
    r = db_client_docs.view('client/running_balance', key=leads_id)
    
    if len(r.rows) == 0:
        running_balance = Decimal(0)
    else:
        running_balance = Decimal('%0.2f' % r.rows[0].value)
        
        

    fname = string.capwords(string.strip(subcon.fname))
    lname = string.capwords(string.strip(subcon.lname))
    
    #get fname, lname
    sql = text("""SELECT fname, lname, email, registered_domain from leads
        WHERE id = :leads_id
        """)
    client_fname, client_lname, client_email, registered_domain = conn.execute(sql, leads_id = leads_id).fetchone()
    
    qty = running_balance
    
    doc_id = None
    order_id = None
    result_msg = ""    
    #If running balance of client is greater than zero, Just send an email notification for refund
    if qty > 0:
        
        #Send email to Accounts , SC and Edward
        to =[]
        to.append('accounts@remotestaff.com.au')
        to.append('edward@remotestaff.com.au')
        
        #Get Assigned SC of client
        sql = text("""SELECT a.admin_id, a.admin_email, a.status FROM leads AS l
                JOIN admin AS a
                ON l.csro_id = a.admin_id
                WHERE l.id = :client_id
                """)
        admin_data = conn.execute(sql, client_id=leads_id).fetchone()
         
        #result_csro_email = send_task("EmailSender.GetCsroEmail", [doc['client_id']])
        #csro_email = result_csro_email.get()
        if admin_data != None:
            admin_id, admin_email, admin_status = admin_data
            to.append(admin_email)
            
        logging.info('sending email notification for refund')
        message = "<p>Hi Team,</p>"
        message += "<p>Your client %s %s cancelled the contract with subcon %s %s and client's available is %s %s </p>" % (client_fname, client_lname, fname, lname , couch_currency, running_balance) 
        message += "<p>Please issue a refund.</p>"
        message += "<p>RS System</p>"
        
        doc = dict(
            to = to,
            bcc = ['devs@remotestaff.com.au'],
            created = get_ph_time(as_array=True),
            generated_by = 'celery task prepaid_create_invoice.create_client_final_invoice',
            text = None,
            html = message,
            subject = 'Available Balance For Refund for Client  %s %s' % (client_fname, client_lname),
            sent = False,
        )
        doc['from'] = "No Reply<noreply@remotestaff.com.au>"
    
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['mailbox']
        db.save(doc)
        
        doc_id = None
        order_id = None
        result_msg = "Client available balance is for refund. Email sent to Accounts and assigned Staffing Consultant."
    
    #if running balance of client is below zero. create final invoice and send email notification
    if qty < 0:
        
        #convert negative to positive running balance
        qty = abs(qty)
            
        #Breakdown the running balance of Client
        gst_amount = Decimal('0.00')
        qty_before_gst = qty
        #check if we must remove gst first
        if apply_gst == 'Y':
            qty_before_gst = qty / Decimal('1.1')
            gst_amount = qty - qty_before_gst
            qty = qty_before_gst 
        
                
        if subcon.work_status == 'Part-Time':
            hours_per_day = 4
        else:
            hours_per_day = 8
        
        #return_date = final_invoice.pay_hrs_to_staff_start_date - timedelta(days = 1)
        #week_days = get_week_days(return_date, final_invoice.pay_hrs_to_staff_end_date)
        #working_days = int(week_days)
        #total_hours = Decimal('%0.2f' % (working_days * hours_per_day))
        
        
        staff_hourly_rate = Decimal('%0.2f' % (subcon.client_price * 12.0 / 52.0 / 5.0 / hours_per_day))
        hourly_rate_total = staff_hourly_rate
        
        proportion = staff_hourly_rate / hourly_rate_total
        
        shared_amount = qty_before_gst * Decimal('%0.2f' % proportion)
        approximate_hour = shared_amount / staff_hourly_rate
        
        total_staff_hourly_rate = staff_hourly_rate
        
        
        #override_hours_per_invoice = Decimal('%s' % total_hours)
        
        
        history = []
        #rate_changes = []
    
        invoice_items = []
        #start_date = final_invoice.pay_hrs_to_staff_start_date
        #end_date_item = final_invoice.pay_hrs_to_staff_end_date
        #amount = Decimal(total_hours * staff_hourly_rate) 
            
        
        invoice_items.append(dict(
            item_id = 1,
            #start_date = None,#[start_date.year, start_date.month, start_date.day],
            #end_date = None, #[end_date_item.year, end_date_item.month, end_date_item.day],
            unit_price = '%0.2f' % staff_hourly_rate,
            qty = '%0.2f' % approximate_hour,
            amount = '%0.2f' % shared_amount,
            subcontractors_id = int(subcon_id),
            description = 'Final Invoice for the unpaid balance of  %s %s [%s]' % (fname, lname, subcon.job_designation),
            staff_name = "%s %s" % (fname, lname),
            work_status = "%s" % subcon.work_status,
            job_designation = "%s" % subcon.job_designation,                       
        ))
        
        
        #sub_total = amount
        
               
        #gst_amount = Decimal(0)
        #total_amount = sub_total
        #if apply_gst == 'Y':
        #    gst_amount = sub_total * Decimal('0.1')
        #    total_amount = total_amount + gst_amount
            
        #get last order id
        r = db_client_docs.view('client/last_order_id', startkey=[leads_id, "%s-999999999" % leads_id],
            endkey=[leads_id, ""], descending=True, limit=1)
    
        if len(r.rows) == 0:
            last_order_id = 1
        else:
            last_order_id_str = r.rows[0].key[1]
            x, last_order_id = string.split(last_order_id_str, '-')
            last_order_id = int(last_order_id)
            last_order_id += 1
    
        order_id = '%s-%08d' % (leads_id, last_order_id)    
        
        #3 days max
        start_date_day_after = get_ph_time() + timedelta(days=2)
    
        pay_before_date = [
            start_date_day_after.year, 
            start_date_day_after.month, 
            start_date_day_after.day, 
            start_date_day_after.hour, 
            start_date_day_after.minute, 
            start_date_day_after.second, 
        ]
    
        doc_order = dict(
            added_by = added_by,
            apply_gst = apply_gst,
            client_id = leads_id,
            history = history,
            type = 'order',
            added_on = now,
            items = invoice_items,
            status = 'new',
            order_id = order_id,
            sub_total = '%0.2f' % qty,
            total_amount = '%0.2f' % (qty + gst_amount),
            gst_amount = '%0.2f' % gst_amount,
            client_fname = client_fname,
            client_lname = client_lname,
            client_email = client_email,
            registered_domain = registered_domain, 
            currency = couch_currency,
            pay_before_date = pay_before_date,
            final_invoice = True,
            final_invoice_id = int(final_invoice_id)
        )
    
        doc_order['running_balance'] = '%0.2f' % running_balance
    
        if len(invoice_items) == 0:
            raise Exception('FAILED to create Client Final Invoice', 'Please check leads_id %s %s' % (leads_id, doc_order))
    
        db_client_docs.save(doc_order)
        logging.info('created client final invoice %s' % doc_order['_id'])
    
        
        #logging.info('sending client final invoice %s' % doc_order['_id'])
        #send_email(doc_order['_id'])
        
        
        #udpate final invoice
        #sql = text("""UPDATE subcontractors_final_invoice SET status = 'sent' , date_sent = :date_sent
        #    WHERE id = :final_invoice_id
        #    """)
        #conn = engine.connect()
        #conn.execute(sql, final_invoice_id = final_invoice_id, date_sent = "%s" % get_ph_time(as_array = False))
    
        #Send email to Accounts , SC and Edward
        to =[]
        to.append('accounts@remotestaff.com.au')
        to.append('edward@remotestaff.com.au')
        
        #Get Assigned SC of client
        sql = text("""SELECT a.admin_id, a.admin_email, a.status FROM leads AS l
                JOIN admin AS a
                ON l.csro_id = a.admin_id
                WHERE l.id = :client_id
                """)
        admin_data = conn.execute(sql, client_id=leads_id).fetchone()
         
        #result_csro_email = send_task("EmailSender.GetCsroEmail", [doc['client_id']])
        #csro_email = result_csro_email.get()
        if admin_data != None:
            admin_id, admin_email, admin_status = admin_data
            to.append(admin_email)
            
        logging.info('sending email notifiction to accounts@remotestaff.com.au, assigned sc and edward')
        message = "<p>Hi Team,</p>"
        message += "<p>Final Invoice for Client %s %s for subcon %s %s has been created with invoice tax number %s</p>" % (client_fname, client_lname, fname, lname , doc_order['order_id']) 
        message += "<p>RS System</p>"
        
        doc = dict(
            to = to,
            bcc = ['devs@remotestaff.com.au'],
            created = get_ph_time(as_array=True),
            generated_by = 'celery task prepaid_create_invoice.create_client_final_invoice',
            text = None,
            html = message,
            subject = 'Created Final Invoice for %s %s [%s]' % (client_fname, client_lname, doc_order['order_id']),
            sent = False,
        )
        doc['from'] = "No Reply<noreply@remotestaff.com.au>"
    
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['mailbox']
        db.save(doc)
        
        doc_id = doc_order['_id']
        order_id = doc_order['order_id']
        result_msg = "Created new Client Final Invoice %s " % doc_order['order_id']

    #close mysql connection        
    conn.close()


    #release redis lock
    redis_client.delete(redis_key)
    
    #invoke xero for invoicing
    sync_invoice_mongo(leads_id)
    #return doc_order.copy()
    
    doc = dict(
        doc_id = doc_id,
        order_id = order_id,
        result_msg = result_msg
    )
    return doc


def send_email(doc_id):
    """given the doc_id, send email
    """
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to send email.', 'Document %s not found.' % doc_id)

    #get client emails
    result = send_task("EmailSender.GetEmails", [doc['client_id']])
    email_recipients = result.get()
    email_to_list = email_recipients['recipients']

    #default cc list
    email_cc_list = []

    #add the assigned csro email
    result_csro_email = send_task("EmailSender.GetCsroEmail", [doc['client_id']])
    csro_email = result_csro_email.get()
    if csro_email != None:
        email_cc_list.append(csro_email)

    #bcc devs
    email_bcc_list = ['devs@remotestaff.com.au']
    email_from = 'accounts@remotestaff.com.au'

    #send celery task
    send_task('prepaid_send_invoice.send', [doc_id, email_to_list, email_cc_list, email_bcc_list, email_from])

    return 'ok'

def sync_invoice_mongo(leads_id):
    
    return True
    
    
    import pycurl
    c = pycurl.Curl()
    c.setopt(c.URL, settings.API_URL+'/mongo-index/client-settings-today-by-id/')
    try:
        # python 3
        from urllib.parse import urlencode
    except ImportError:
        # python 2
        from urllib import urlencode
    
    
    post_data = {'leads_id': leads_id}
    # Form data must be provided already urlencoded.
    postfields = urlencode(post_data)
    # Sets request method to POST,
    # Content-Type header to application/x-www-form-urlencoded
    # and data to send in request body.
    c.setopt(c.POSTFIELDS, postfields)
    
    c.setopt(pycurl.SSL_VERIFYPEER, 1)
    c.setopt(pycurl.SSL_VERIFYHOST, 2)
    c.setopt(pycurl.CAINFO, certifi.where())
    
    c.perform()
    c.close()

@task    
def create_first_month_invoice_from_service_agreement(service_agreement_id):
    
    #get items
    sql = text("""SELECT s.service_agreement_id, s.quote_id, s.leads_id
        FROM service_agreement as s
        WHERE s.service_agreement_id = :service_agreement_id
        """)
    conn = engine.connect()
    service_agreement = conn.execute(sql, service_agreement_id = service_agreement_id).fetchone()   
    quote_id = service_agreement.quote_id
    leads_id = service_agreement.leads_id
    
    leads_id = int(leads_id)
    
    #couchdb settings
    s = couchdb.Server(settings.COUCH_DSN)
    db_client_docs = s['client_docs']

    #check if client has couchdb settings
    now = get_ph_time(as_array = True)
    r = db_client_docs.view('client/settings', startkey=[leads_id, now],
        endkey=[leads_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)

    if len(r.rows) == 0:    #no client settings, send alert
        raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))

    data = r.rows[0]
    doc = data.doc
    couch_currency, apply_gst = data['value']
    #return couch_currency
    if doc.has_key('days_to_invoice'):
        working_days = doc['days_to_invoice']
    else:
        working_days = WORKING_WEEKDAYS 
    
    days_before_suspension=0
    
    if doc.has_key('days_before_suspension'):
        days_before_suspension = doc['days_before_suspension']
    
    #Prepaid clients only
    if days_before_suspension != -30:    
    
        deduct_running_balance = False
        added_by = "celery prepaid_create_invoice.create_first_month_invoice_from_service_agreement"
        
        #check locks using redis
        
        redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        redis_key = 'create_invoice_lock:%s' % leads_id
        lock = redis_client.setnx(redis_key, leads_id)
        if lock == False:
            raise Exception('FAILED to acquire lock for %s' % redis_key, 'FAILED to acquire lock for %s' % redis_key)

        #add expire in case creation of invoice fails
        redis_client.expire(redis_key, 5)
        
        #get items
        sql = text("""SELECT s.id, s.client_price, s.currency,
            s.job_designation, s.work_status, s.starting_date,
            p.fname, p.lname
            FROM subcontractors as s
            LEFT JOIN personal AS p
            ON s.userid = p.userid
            WHERE s.leads_id = :leads_id
            AND s.service_agreement_id = :service_agreement_id
            AND s.status in ('pending')
            """)
        conn = engine.connect()
        items = conn.execute(sql, leads_id = leads_id, service_agreement_id = service_agreement_id).fetchall()
        
        sub_total = Decimal(0)
        invoice_items = []
        currency_check = []
    
        #check clients running balance
        r = db_client_docs.view('client/running_balance', key=leads_id)
        
        if len(r.rows) == 0:
            running_balance = Decimal(0)
        else:
            running_balance = Decimal('%0.2f' % r.rows[0].value)
    
        if len(items) == 0:
            logging.info('No items found for leads_id %s' % leads_id)
        
        import ClientsWithPrepaidAccounts
        clients_daily_rate = ClientsWithPrepaidAccounts.get_clients_daily_rate(leads_id)
        #r = send_task("ClientsWithPrepaidAccounts.get_clients_daily_rate", [leads_id,])
        #clients_daily_rate = r.get()
    
        #given the running_balance and clients_daily_rate, get possible number of days for the start_date
        if clients_daily_rate == 0:
            max_days = 0
        else:
            max_days = int(running_balance / clients_daily_rate)
            if max_days < 0:
                max_days = 0
    
        #start_date = add_week_days(get_ph_time(), max_days)
        #zero out hours/minutes/seconds
        #start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
        #end_date = add_week_days(start_date, working_days)
    
        history = []
        i = 1
        rate_changes = []
    
        #load currency adjustment module
        import currency_adjustment
        
        #store items for proper currency adjustment allocation
        currency_adjustment_list = []
        
        for item in items:
            sid, client_price, currency, job_designation, work_status, starting_date, fname, lname = item
            
            #convert starting_date to datetime format, currently its on date format
            starting_date = datetime(starting_date.year, starting_date.month, starting_date.day, 0, 0, 0)
            start_date = starting_date
            #start_date = add_week_days(starting_date, max_days)
            
            #zero out hours/minutes/seconds
            #start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
            end_date = add_week_days(start_date, working_days)
    
            #skip item if starting_date hasn't started yet
            now_date = get_ph_time(as_array=False)
            #if starting_date > now_date:
            #    send_task('skype_messaging.notify_skype_id', ['skipped invoice item since it has not started yet \n: %s' % pformat(item.items(), 4), 'locsunglao'])     #TODO remove once stable
            #    continue
    
            fname = string.capwords(string.strip(fname))
            lname = string.capwords(string.strip(lname))
    
            if couch_currency != currency:
                raise Exception('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))
    
            if work_status == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8
    
            total_hours = Decimal('%0.2f' % (working_days * hours_per_day))
            staff_hourly_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0 / hours_per_day))
            override_hours_per_invoice = Decimal('%s' % total_hours)
        
        
            end_date_item = end_date
    
            amount = Decimal(total_hours * staff_hourly_rate)            
            
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                subcon_id = sid,
                description = '%s %s [%s]' % (fname, lname, job_designation),
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                )
    
    
            invoice_items.append(invoice_item)
    
            sub_total += amount
    
            i += 1
            
            #currency adjustment items
            forex = currency_adjustment.get_forex_rate_per_staff(sid)
            subcon = currency_adjustment.get_contract_detail(sid)
            currency_difference = currency_adjustment.get_currency_adjustment_per_staff(sid)
            currency_difference_peso = currency_adjustment.get_currency_adjustment_peso_per_staff(sid)
            
            couch_currency, couch_apply_gst = currency_adjustment.get_currency_apply_gst_client(leads_id)
            forex_rate = float(forex.rate)
            subcon_current_rate = float(subcon.current_rate)
            
            amount = Decimal(currency_difference) * total_hours
            description = "Currency Adjustment (Contract Rate 1 %s = %.02f PESO vs. Current Rate 1 %s = %.02f PESO, Currency Difference of %.02f PESO for your staff %s [%s])" % (couch_currency, subcon_current_rate, couch_currency,forex_rate,currency_difference_peso,  fname+" "+lname, job_designation)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date_item.year, end_date_item.month, end_date_item.day],
                unit_price = '%0.4f' % currency_difference,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = description,
                subcon_id = sid,
                staff_name = "%s %s" % (fname, lname),
                job_designation = "%s" % job_designation,
                )
            invoice_items.append(invoice_item)
            sub_total += amount
            
            currency_adjustment_list.append(dict(
                subcon_id = sid,
                currency_difference = currency_difference,
                start_date = start_date,
                end_date = end_date_item,
                total = float("{0:.2f}".format(amount))
            ))
            
            i+=1
    
        if deduct_running_balance == True:
            if running_balance > Decimal(0):    #deduct running balance
                now_date = datetime(now[0], now[1], now[2])
                invoice_item = dict(
                    item_id = i,
                    unit_price = '%0.2f' % running_balance,
                    qty = '-1.00',
                    amount = '%0.2f' % (running_balance * Decimal('-1.00')),    #negative amount
                    description = 'Credit From Available Balance as of %s' % now_date.strftime('%b %d, %Y')
                )
    
                invoice_items.append(invoice_item)
                sub_total += running_balance * Decimal('-1.00')
    
        #get fname, lname
        sql = text("""SELECT fname, lname, email, registered_domain from leads
            WHERE id = :leads_id
            """)
        client_fname, client_lname, client_email, registered_domain = conn.execute(sql, leads_id = leads_id).fetchone()
    
        gst_amount = Decimal(0)
        total_amount = sub_total
        if apply_gst == 'Y':
            gst_amount = sub_total * Decimal('0.1')
            total_amount = total_amount + gst_amount
    
        #get last order id
        r = db_client_docs.view('client/last_order_id', startkey=[leads_id, "%s-999999999" % leads_id],
            endkey=[leads_id, ""], descending=True, limit=1)
    
        if len(r.rows) == 0:
            last_order_id = 1
        else:
            last_order_id_str = r.rows[0].key[1]
            x, last_order_id = string.split(last_order_id_str, '-')
            last_order_id = int(last_order_id)
            last_order_id += 1
    
        order_id = '%s-%08d' % (leads_id, last_order_id)
    
        start_date_day_after = get_ph_time() + timedelta(days=4)
    
        pay_before_date = [
            start_date_day_after.year, 
            start_date_day_after.month, 
            start_date_day_after.day, 
            start_date_day_after.hour, 
            start_date_day_after.minute, 
            start_date_day_after.second, 
        ]
    
        doc_order = dict(
            added_by = added_by,
            apply_gst = apply_gst,
            client_id = leads_id,
            history = history,
            type = 'order',
            added_on = now,
            items = invoice_items,
            status = 'new',
            order_id = order_id,
            sub_total = '%0.2f' % sub_total,
            total_amount = '%0.2f' % total_amount,
            gst_amount = '%0.2f' % gst_amount,
            client_fname = client_fname,
            client_lname = client_lname,
            client_email = client_email,
            registered_domain = registered_domain, 
            currency = currency,
            pay_before_date = pay_before_date,
        )
    
        doc_order['running_balance'] = '%0.2f' % running_balance
    
        if len(invoice_items) == 0:
            raise Exception('FAILED to create Prepaid Based Invoice', 'Please check leads_id %s %s' % (leads_id, doc_order))
    
        db_client_docs.save(doc_order)
        logging.info('created first month invoice %s' % doc_order['_id'])
    
        
        logging.info('sending first month invoice %s' % doc_order['_id'])
        send_email(doc_order['_id'])
        
        
        logging.info("sending email notification to accounts")
        
        message = "<p>Hi Team,</p>"
        message += "<p>&nbsp;</p>"
        message += "<p>First month invoice has been created for client %s %s.</p>" % (client_fname, client_lname)
        message += "<p>&nbsp;</p>"
        message += "<p>Please check Invoice Number %s.</p>" % order_id
        message += "<p>&nbsp;</p>"
        message += "<p>This is system generated. Please do not reply to this email.</p>"
        message += "<p>&nbsp;</p>"
        message += "<p>RS System</p>"
        
        doc = dict(
            to = ['accounts@remotestaff.com.au'],
            bcc = ['devs@remotestaff.com.au'],
            created = get_ph_time(as_array=True),
            generated_by = 'celery task prepaid_create_invoice.create_first_month_invoice_from_service_agreement',
            text = None,
            html = message,
            subject = 'First Month Invoice for Client  %s %s' % (client_fname, client_lname),
            sent = False,
        )
        doc['from'] = "No Reply<noreply@remotestaff.com.au>"
    
        s = couchdb.Server(settings.COUCH_DSN)
        db = s['mailbox']
        db.save(doc)
            
            
        conn.close()


        #release redis lock
        redis_client.delete(redis_key)
    
        #invoke xero for invoicing
        sync_invoice_mongo(leads_id)
        return doc_order.copy()
        
        
           

def run_tests():
    """
    >>> create_first_month_invoice_from_service_agreement(4666)
    >>> doc_order = create_invoice(8819, WORKING_WEEKDAYS, False, 'test celery prepaid_on_finish_work')
    >>> logging.info('%s' % pformat(doc_order, 4))
    >>> doc_order = create_invoice(5439, WORKING_WEEKDAYS, False, 'test celery prepaid_on_finish_work')
    >>> logging.info('%s' % pformat(doc_order, 4))
    >>> doc_order = create_invoice(7219, WORKING_WEEKDAYS, False, 'test celery prepaid_on_finish_work')
    >>> logging.info('%s' % pformat(doc_order, 4))
    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
