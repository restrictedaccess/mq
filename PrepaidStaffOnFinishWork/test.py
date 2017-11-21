#!/usr/bin/env python
#   2012-08-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   extend create_invoice function to enable number of days parameter
#   2012-08-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added cristina.e@remotestaff.com.au as per ricas request
#   2012-06-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on retrieving items that are not ACTIVE
#   2012-05-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added csro email
#   -   bugfix on retrieving csro_email
#   2012-04-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added csro email to the invoice notification
#   2012-04-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bccd accounts on production
#   2012-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added set_second_day_notice function
#   2012-03-28  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   issue invoice 2 days worth of load
#   2012-03-21  Lawrence Sunglao<lawrence.sunglao@remotestaff.com.au>
#   -   updated particular and remarks as per ricas instruction
#   2012-03-13  Lawrence Sunglao<lawrence.sunglao@remotestaff.com.au>
#   -   Changed to 2 days before it disables rssc, used settings.py

import MySQLdb
import couchdb
import settings
import string

from datetime import date, datetime, timedelta
from pytz import timezone
from celery.execute import send_task

from pprint import pprint, pformat

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

from Cheetah.Template import Template

from decimal import Decimal
import math

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

WORKING_WEEKDAYS = 22


class PrepaidOnFinishWork:
    def __send_email_alert__(self, subject, msg):
        logging.info('%r ::: %r' % (subject, msg))
        msg = MIMEText(msg)
        if settings.DEBUG:
            msg['Subject'] = 'TEST %s' % subject
        else:
            msg['Subject'] = subject

        msg['From'] = 'remotestaff rabbitmq<noreply@remotestaff.com.au>'
        msg['To'] = settings.EMAIL_ALERT

        s = smtplib.SMTP(host= settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )
        s.sendmail('noreply@remotestaff.com.au',
            settings.EMAIL_ALERT, msg.as_string())
        s.quit()


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


    def add_worked_timerecord(self, doc_time_record_id):
        """generates a charge couchdb document and returns a copy of document
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db_time_records = s['rssc_time_records']

        #get timerecord doc
        doc_time_record = db_time_records.get(doc_time_record_id)
        if doc_time_record == None:
            self.__send_email_alert__('ERROR PREPAID TIMERECORD',
                'Please check time_record doc_id %s' % doc_time_record_id)
            return

        #check if prepaid
        db = s['rssc']
        doc_subcon = db.get('subcon-%s' % doc_time_record['subcontractors_id'])

        if doc_subcon.has_key('prepaid') == False:
            return

        prepaid = doc_subcon['prepaid']
        if prepaid == 'no':
            return

        leads_id = doc_subcon['leads_id']

        #check if leads_id matches
        if doc_time_record['leads_id'] != leads_id:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check leads_id for rssc %s. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        #check if client has couchdb settings
        db_client_docs = s['client_docs']
        now = self.__get_phil_time__(as_array = True)
        r = db_client_docs.view('client/settings', startkey=[leads_id, now],
            endkey=[leads_id, [2011,1,1,0,0,0,0]], 
            descending=True, limit=1)

        if len(r.rows) == 0:    #no client settings, alert devs
            self.__send_email_alert__('Failed to credit on finish work', 'Please check client settings for leads_id %s. Failed to credit doc rssc_time_records %s' % (leads_id, doc_time_record_id))
            return

        currency, apply_gst = r.rows[0]['value']
        
        #apply_gst check
        if doc_subcon['client'].has_key('apply_gst') == False:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Missing apply_gst on doc_subcon. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        if apply_gst == 'Y' and doc_subcon['client']['apply_gst'] == 'no':
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Inconsistent apply_gst on doc_subcon. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        if apply_gst == 'N' and doc_subcon['client']['apply_gst'] == 'yes':
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Inconsistent apply_gst on doc_subcon. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        #currency check
        if doc_subcon.has_key('client_currency') == False:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Missing client_currency field. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        if currency != doc_subcon['client_currency']:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Inconsistent client_currency. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        #check client_price_hourly_rate
        if doc_subcon.has_key('client_price_hourly_rate') == False:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Missing client_price_hourly_rate. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        #check userid consistency
        if doc_subcon['userid'] != doc_time_record['userid']:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. Inconsistent userids. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        a = doc_time_record['time_in']
        time_in = datetime(a[0], a[1], a[2], a[3], a[4], a[5])

        b = doc_time_record['time_out']
        time_out = datetime(b[0], b[1], b[2], b[3], b[4], b[5])

        if time_out < time_in:
            self.__send_email_alert__('Failed to credit on finish work', 'Please check rssc %s. time_out is less thant time_in. Failed to credit doc rssc_time_records %s' % (doc_subcon['_id'], doc_time_record_id))
            return

        total_time = time_out - time_in

        userid = doc_subcon['userid']
        r = db_time_records.view('summary/userid_lunch_start', 
            startkey=[userid, doc_time_record['time_in']], 
            endkey=[userid, doc_time_record['time_out']],
            include_docs=True)


        lunch_time = timedelta(0)

        for x in r:
            lunch_doc = x.doc
            a = lunch_doc['start']
            b = lunch_doc['end']
            start = datetime(a[0], a[1], a[2], a[3], a[4], a[5])
            end = datetime(b[0], b[1], b[2], b[3], b[4], b[5])
            lunch_time += end - start

        total_time -= lunch_time
        total_time_decimal = Decimal('%0.2f' % (total_time.seconds / 3600.0))
        r = db_client_docs.view('client/running_balance', key=leads_id)

        if len(r.rows) == 0:
            running_balance = 0
        else:                                                               
            running_balance = Decimal('%s' % r.rows[0].value)

        amount = Decimal(doc_subcon['client_price_hourly_rate']) * total_time_decimal
        charge = Decimal(amount)

        particular = 'Staff %s %s, <%s> worked for %0.2f hours @ %s/hr' % (
            doc_subcon['staff']['fname'], 
            doc_subcon['staff']['lname'], 
            doc_subcon['job_designation'], 
            total_time_decimal, 
            doc_subcon['client_price_hourly_rate'],
            )

        #apply_gst
        if apply_gst == 'Y':
            charge += charge * Decimal('0.1')
            particular += ' plus GST'

        running_balance -= charge

        doc_transaction = dict(
            added_by = 'RSSC Prepaid On Finish Work',
            added_on = now,
            charge = '%0.2f' % charge,
            client_id = leads_id,                                          
            credit = '0.00',
            credit_type = 'WORK',
            currency = doc_subcon['client_currency'],
            remarks = 'Generated from time sheet',
            type = 'credit accounting',
            running_balance = '%0.2f' % running_balance,
            particular = particular,
            doc_subcon = doc_subcon['_id'],
            doc_time_record_id = doc_time_record_id,
        )

        db_client_docs.save(doc_transaction)
        return doc_transaction.copy()


    def get_clients_daily_rate(self, leads_id):
        """given the leads_id, return the daily rate in decimal
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db_client_docs = s['client_docs']

        #check if client has couchdb settings
        now = self.__get_phil_time__(as_array = True)
        r = db_client_docs.view('client/settings', startkey=[leads_id, now],
            endkey=[leads_id, [2011,1,1,0,0,0,0]], 
            descending=True, limit=1)

        if len(r.rows) == 0:    #no client settings, create one
            self.__send_email_alert__('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))
            return

        couch_currency, apply_gst = r.rows[0]['value']

        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()

        #get items
        c.execute("""SELECT s.id, s.client_price, s.currency,
            s.job_designation, s.work_status, 
            p.fname, p.lname
            FROM subcontractors as s
            LEFT JOIN personal AS p
            ON s.userid = p.userid
            WHERE s.leads_id = %s
            AND prepaid='yes'
            AND s.status='ACTIVE'
            """ % (leads_id))
        items = c.fetchall()

        daily_rate = Decimal(0)
        for item in items:
            sid, client_price, currency, job_designation, work_status, fname, lname = item
            if couch_currency != currency:
                self.__send_email_alert__('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))
                return

            staff_daily_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0))
            daily_rate += staff_daily_rate

        return daily_rate


    def create_invoice(self, leads_id, working_days=WORKING_WEEKDAYS):
        """generates a couchdb document and returns its document id
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db_client_docs = s['client_docs']

        #check if client has couchdb settings
        now = self.__get_phil_time__(as_array = True)
        r = db_client_docs.view('client/settings', startkey=[leads_id, now],
            endkey=[leads_id, [2011,1,1,0,0,0,0]], 
            descending=True, limit=1)

        if len(r.rows) == 0:    #no client settings, send alert
            self.__send_email_alert__('FAILED to create Prepaid Based Invoice', 'Please check leads_id : %s\r\nNo couchdb client settings found.' % (leads_id))
            return

        couch_currency, couch_apply_gst = r.rows[0]['value']

        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()

        #get items
        c.execute("""SELECT s.id, s.client_price, s.currency,
            s.job_designation, s.work_status, 
            p.fname, p.lname
            FROM subcontractors as s
            LEFT JOIN personal AS p
            ON s.userid = p.userid
            WHERE s.leads_id = %s
            AND prepaid='yes'
            AND s.status='ACTIVE'
            """ % (leads_id))
        items = c.fetchall()

        sub_total = Decimal(0)
        invoice_items = []
        currency_check = []

        start_date = self.__get_phil_time__()
        end_date = self.add_week_days(start_date, working_days)

        i = 1
        for item in items:
            sid, client_price, currency, job_designation, work_status, fname, lname = item

            if couch_currency != currency:
                self.__send_email_alert__('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nCurrency does not match with clients couch settings : %s vs %s' % (sid, couch_currency, currency))
                return

            if work_status == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8

            total_hours = Decimal('%0.2f' % (working_days * hours_per_day))
            staff_hourly_rate = Decimal('%0.2f' % (client_price * 12.0 / 52.0 / 5.0 / hours_per_day))

            amount = Decimal(total_hours * staff_hourly_rate)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date.year, end_date.month, end_date.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = '%s %s [%s] - %0.2fhrs@%0.2f/hr' % (string.capitalize(fname), string.capitalize(lname), job_designation, total_hours, staff_hourly_rate),
                )
            invoice_items.append(invoice_item)

            sub_total += amount

            i += 1

        #get apply_gst, fname, lname
        c.execute("""SELECT apply_gst, fname, lname, email, registered_domain  from leads
            where id = %s
            """, (leads_id,))

        apply_gst, client_fname, client_lname, client_email, registered_domain = c.fetchone()

        if apply_gst == 'yes':
            apply_gst = 'Y'
        else:
            apply_gst = 'N'

        if apply_gst != couch_apply_gst:
            self.__send_email_alert__('FAILED to create Prepaid Based Invoice', 'Please check subcontractors.id : %s\r\nGST mismatch with couchdb' % (sid))
            return

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

        doc_order = dict(
            added_by = 'MQ PrepaidStaffOnFinishWork.py',
            apply_gst = apply_gst,
            client_id = leads_id,
            history = [],
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
        )

        #check clients running balance
        r = db_client_docs.view('client/running_balance', key=leads_id)
        
        if len(r.rows) == 0:
            running_balance = Decimal(0)
        else:
            running_balance = Decimal('%0.2f' % r.rows[0].value)

        doc_order['running_balance'] = '%0.2f' % running_balance

        db_client_docs.save(doc_order)

        #return doc_id
        return doc_order['_id']


    def add_week_days(self, reference_date, days):
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

        return return_date


    def generate_html(self, doc_id):
        """given the doc_id, return an html message for email
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']

        doc = db.get(doc_id)
        if doc == None:
            self.__send_email_alert__('FAILED to create HTML', 'Please check doc_id:%s sent for generate_html function.' % (doc_id))
            return

        #get the template
        t = Template(file="prepaid_invoice_on_deplete_account.tmpl")

        #assign variables to the template file
        t.client_fname = doc['client_fname']
        t.client_lname = doc['client_lname']
        x = doc['added_on']
        latest_payment_date = date(x[0], x[1], x[2])
        latest_payment_date = latest_payment_date + timedelta(days = settings.DAYS_BEFORE_INVOICE_ISSUE)
        t.latest_payment_date = latest_payment_date

        #get currency_sign
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("""SELECT sign from currency_lookup
            WHERE code = %s
            """, doc['currency'])
        currency_sign = c.fetchone()[0]
        t.currency_sign = currency_sign

        #get client_data
        c.execute("""SELECT company_name, company_address
            FROM leads
            WHERE id = %s
            """, doc['client_id'])
        client_data = c.fetchone()
        t.company_name = client_data[0]
        t.company_address = client_data[1]

        items_converted_date = []
        items = doc['items']
        for item in items:
            a = item.copy()
            b = a['start_date']
            a['start_date'] = date(b[0], b[1], b[2])
            c = a['end_date']
            a['end_date'] = date(c[0], c[1], c[2])
            amount = Decimal(a['amount'])
            a['amount'] = locale.format('%0.2f', amount, True)

            items_converted_date.append(a)


        t.invoice_items = items_converted_date
        t.order_id = doc['order_id']
        t.sub_total = locale.format('%0.2f', Decimal(doc['sub_total']), True)
        t.currency = doc['currency']
        t.gst_amount = locale.format('%0.2f', Decimal(doc['gst_amount']), True)
        t.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)
        t.DEBUG = settings.DEBUG
        t.doc_id = doc['_id']

        return '%s' % t


    def send_email(self, doc_id):
        """given the doc_id, send email
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']

        doc = db.get(doc_id)
        if doc == None:
            return

        result = send_task("EmailSender.GetEmails", [doc['client_id']])
        email_recipients = result.get()

        html_message = self.generate_html(doc_id)

        msg = MIMEMultipart()
        part1 = MIMEText('%s' % html_message, 'html')
        part1.add_header('Content-Disposition', 'inline')
        msg.attach(part1)
        msg['couch_doc_id'] = doc_id
        msg['To'] = email_recipients['to']
        if email_recipients['cc'] != '':
            msg['Cc'] = email_recipients['cc']

        msg['From'] = 'noreply@remotestaff.com.au'

        subject = 'REMOTE STAFF PREPAID ORDER # %s' % doc['order_id']
        recipients = email_recipients['recipients']
        recipients.append('devs@remotestaff.com.au')
        recipients.append('accounts@remotestaff.com.au')
        recipients.append('orders@remotestaff.com.au')
        recipients.append('cristina.e@remotestaff.com.au')

        #add the assigned csro email
        result_csro_email = send_task("EmailSender.GetCsroEmail", [doc['client_id']])
        csro_email = result_csro_email.get()
        if csro_email != None:
            recipients.append(csro_email)

        if settings.DEBUG:
            subject = 'TEST %s' % subject
            recipients = [settings.EMAIL_ALERT]

        msg['Subject'] = subject

        s = smtplib.SMTP(host = settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )

        s.sendmail('noreply@remotestaff.com.au', 
            recipients,
            msg.as_string())
        s.quit()

        #add history that the document was sent
        doc = db.get(doc_id)
        if doc.has_key('history') == False:
            history = []
        else:
            history = doc['history']

        history.append(dict(
            timestamp = self.__get_phil_time__().strftime('%F %H:%M:%S'),
            changes = 'Email sent to %s.' % doc['client_email'],
            by = 'MQ on deplete client account'
        ))

        doc['history'] = history
        db.save(doc)
        return 'ok'


    def has_open_invoice(self, leads_id, days):
        """check for open invoice for the past x days
        returns None else, returns a copy of the document
        just for checking the second_day notice
        """
        now = self.__get_phil_time__(as_array = False)
        prev_date = now - timedelta(days=days)

        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']

        r = db.view('client/mq_created_order', 
            startkey=[leads_id, 
                [prev_date.year, 
                    prev_date.month, 
                    prev_date.day, 
                    prev_date.hour, 
                    prev_date.minute, 
                    prev_date.second, 
                    0]],
            endkey=[leads_id,
                [now.year,
                    now.month,
                    now.day,
                    now.hour,
                    now.minute,
                    now.second,
                    0]],
            include_docs=True)

        if len(r.rows) == 0:
            return None
        else:
            return r.rows[0].doc.copy()


    def set_second_day_notice(self, doc_id):
        """given the doc_id, add field second_day_notice
        """
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']
        doc = db.get(doc_id)
        if doc == None:
            self.__send_email_alert__('FAILED to set second_day_notice', 'Please check doc_id:%s.' % (doc_id))
            return
        doc['second_day_notice'] = self.__get_phil_time__().strftime('%F %H:%M:%S')
        db.save(doc)


if __name__ == '__main__':
    prepaid = PrepaidOnFinishWork()
##~    prepaid.add_worked_timerecord('FAIL TIME RECORD')
##~    pprint(prepaid.add_worked_timerecord('sample_time_record'))
    pprint(prepaid.add_worked_timerecord('21ab31a424e6a63b713ca93d50fe8c95'))
##~    pprint(prepaid.get_clients_daily_rate(11))
##~    pprint(prepaid.create_invoice(11))
##~    pprint(prepaid.create_invoice(11, 5))    #test for 5 days invoice
##~    prepaid.send_email('758634bf08d3ff85dec6bab00100a76a')
##~    prepaid.send_email('8e3664ab9a618075c8c6e92946001085')
##~    prepaid.send_email('8e3664ab9a618075c8c6e9294600025c')
##~    print prepaid.has_open_invoice(11, 5)
##~    prepaid.set_second_day_notice('FAIL INVOICE TEST')

