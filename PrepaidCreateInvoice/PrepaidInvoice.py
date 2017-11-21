#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-10-25 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed accounts
#   2012-10-11 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed auto set to paid
#   2012-10-10 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a pay_before_date field that would reflect on invoice
#   -   added history if current balance can cover the invoice
#   -   removed total hours and hourly rate on item description
#   2012-10-03 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed cc orders@remotestaff.com.au
#   2012-09-27 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   upgraded to use the celery task for sending invoice and removed the template
#   2012-04-20 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   manually updated leads.apply_gst
#   2012-04-17 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   force apply_gst to N if not AUD
#   2012-04-12 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on retrieving leads_id
#   2012-04-09 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   send an email if leads_id does not exists
#   2012-03-21 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added accounts to the recepient
#   2012-03-11 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updates on using auto-login on invoice payment and autoresponders
#   2012-02-09 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   referred to prepaid_start_date as the start_date
#   2012-01-27 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   reconsidered previous balance
#   2012-01-19 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed previous balance checks
#   2011-12-22 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on earliest_date
#   -   add subcontractors_invoice_setup__id on created order doc

import MySQLdb
import couchdb
import settings
import pika
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
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


WORKING_WEEKDAYS = 22


class PrepaidInvoice:
    def __send_email_alert__(self, subject, msg):
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


    def send_email(self, doc_id):
        """given the doc_id, send email
        """
        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']

        doc = db.get(doc_id)
        if doc == None:
            return

        task = send_task('EmailSender.GetPlainEmails', [doc['client_id']])
        task_result = task.get()
        emails_to = task_result['to']
        emails_cc = task_result['cc']

        #get csro email and cc
        task = send_task('EmailSender.GetCsroEmail', [doc['client_id']])
        task_result = task.get()
        if task_result != None:
            emails_cc.append(task_result)

        emails_bcc = ['devs@remotestaff.com.au',]
        email_from = 'accounts@remotestaff.com.au'

        #send celery task
        send_task('prepaid_send_invoice.send', [doc_id, emails_to, emails_cc, emails_bcc, email_from])

        #add history that the document was sent via celery
        doc = db.get(doc_id)
        if doc.has_key('history') == False:
            history = []
        else:
            history = doc['history']

        history.append(dict(
            timestamp = self.__get_phil_time__().strftime('%F %H:%M:%S'),
            changes = 'send_task prepaid_send_invoice.send',
            by = 'MQ First Month Invoice'
        ))

        doc['history'] = history
        db.save(doc)
        return doc['status']


    def resend_email(self, sid):
        """given the subcontractors_invoice_setup.id
        search client_docs couchdb, get doc_id and send email
        """
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db = s['client_docs']
        now = self.__get_phil_time__(as_array = True)
        r = db.view('client/subcontractors_invoice_setup_id', startkey=[sid,now], endkey=[sid,[0,0,0,0,0,0,0]], limit=1, descending=True)

        if len(r.rows) == 0:    #no client settings, create one
            self.__send_email_alert__('Not able to find : %s for resending invoice.' % sid, 'Not able to find : %s for resending invoice.' % sid)
            return

        doc_id = r.rows[0]['id']
        self.send_email(doc_id)


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


    def create_invoice(self, sid):
        """generates a couchdb document and returns its document id
        """
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()

        #get leads_id
        c.execute("""SELECT leads_id from subcontractors_invoice_setup
            WHERE id = %s
            """, sid)
        result = c.fetchone()
        if result == None:
            self.__send_email_alert__('ALERT Not able to find leads_id from subcontractors_invoice_setup : %s for creating invoice.' % sid, 'Not able to find leads_id from subcontractors_invoice_setup : %s for creating invoice.' % sid)
            return

        leads_id = result[0]

        #get subcontractors_temp_ids
        c.execute("""SELECT subcontractors_id 
            FROM subcontractors_invoice_setup_details
            WHERE subcontractors_invoice_setup_id = %s""", (sid,))
        subcontractors_temp_ids = []
        for x in c.fetchall():
            subcontractors_temp_ids.append(str(x[0]))

        #get items
        c.execute("""SELECT st.client_price, st.currency,
            st.prepaid_start_date, st.job_designation, st.work_status, 
            p.fname, p.lname
            FROM subcontractors_temp as st
            LEFT JOIN personal AS p
            ON st.userid = p.userid
            WHERE st.id IN (%s)
            AND st.leads_id = %s
            """ % (string.join(subcontractors_temp_ids, ','), leads_id))
        items = c.fetchall()

        sub_total = 0
        invoice_items = []
        currency_check = []

        #assume earliest date of 1 year
        earliest_starting_date = date.today() + timedelta(days=365)

        i = 1
        for item in items:
            client_price, currency, start_date, job_designation, work_status, fname, lname = item

            if currency not in currency_check:
                currency_check.append(currency)

            if earliest_starting_date > start_date:
                earliest_starting_date = start_date

            end_date = self.add_week_days(start_date, WORKING_WEEKDAYS)
            if work_status == 'Part-Time':
                hours_per_day = 4
            else:
                hours_per_day = 8

            total_hours = WORKING_WEEKDAYS * hours_per_day
            staff_hourly_rate = client_price * 12 / 52 / 5 / hours_per_day
            staff_hourly_rate = round(staff_hourly_rate, 2)

            amount = round(total_hours * staff_hourly_rate, 2)
            invoice_item = dict(
                item_id = i,
                start_date = [start_date.year, start_date.month, start_date.day],
                end_date = [end_date.year, end_date.month, end_date.day],
                unit_price = '%0.2f' % staff_hourly_rate,
                qty = '%0.2f' % total_hours,
                amount = '%0.2f' % amount,
                description = '%s %s [%s]' % (string.capitalize(fname), string.capitalize(lname), job_designation),
                )
            invoice_items.append(invoice_item)

            sub_total += amount

            i += 1

        #check currency consistency
        if len(currency_check) > 1:
            self.__send_email_alert__('FAILED to create Trial Based First Month Invoice', 'Please check subcontractors_invoice_setup_id : %s\r\nMULTIPLE Currency for leads_id %s' % (sid, leads_id))
            return

        #get apply_gst, fname, lname
        c.execute("""SELECT apply_gst, fname, lname, email, registered_domain  from leads
            where id = %s
            """, (leads_id,))

        apply_gst, client_fname, client_lname, client_email, registered_domain = c.fetchone()

        if apply_gst == 'yes':
            apply_gst = 'Y'
        else:
            apply_gst = 'N'

        #check currency and apply_gst consistency
        if apply_gst == 'Y' and currency != 'AUD':
            apply_gst = 'N'
            #update the leads table as well
            c.execute("""UPDATE leads set apply_gst='no' where id = %s""", (leads_id))
            db.commit()
            self.__send_email_alert__('WARNING apply_gst and currency settings.', "leads_id %s has apply_gst = 'Y' and currency = %s\r\n overriding apply_gst to 'N' and setting leads.apply_gst to 'no'" % (leads_id, currency))

        gst_amount = 0
        sub_total = round(sub_total, 2)
        total_amount = sub_total
        if apply_gst == 'Y':
            gst_amount = round(sub_total * 0.1, 2)
            total_amount = round(total_amount + gst_amount, 2)

        #couchdb settings
        s = couchdb.Server(settings.CLIENT_DOCS_DSN)
        db_client_docs = s['client_docs']

        #check if client has couchdb settings
        now = self.__get_phil_time__(as_array = True)
        r = db_client_docs.view('client/settings', startkey=[leads_id, now],
            endkey=[leads_id, [2011,1,1,0,0,0,0]], 
            descending=True, limit=1)

        if len(r.rows) == 0:    #no client settings, create one
            doc_settings = dict(
                added_by = 'MQ: First Months Invoice',
                apply_gst = apply_gst,
                client_id = leads_id,
                client_fname = client_fname,
                client_lname = client_lname,
                client_email = client_email,
                registered_domain = registered_domain, 
                currency = currency,
                timestamp = now,
                type = 'client settings'
            )
            db_client_docs.save(doc_settings)
            self.__send_email_alert__('Created client_settings document for leads_id : %s' % leads_id, pformat(doc_settings, indent=8))

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
            added_by = 'MQ First Months Invoice',
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
            subcontractors_invoice_setup__id = int(sid),
            earliest_starting_date = [earliest_starting_date.year, 
                earliest_starting_date.month, earliest_starting_date.day],
            pay_before_date = [earliest_starting_date.year, 
                earliest_starting_date.month, earliest_starting_date.day, 0, 0, 0]
        )

        #check if clients running balance can cover the order
        r = db_client_docs.view('client/running_balance', key=leads_id)
        
        if len(r.rows) == 0:
            running_balance = 0
        else:
            running_balance = float(r.rows[0].value)

        TODO_COMMENT_OUT = """
        if running_balance > total_amount:
            doc_order['status'] = 'paid'
            doc_order.pop('pay_before_date')
            doc_order['history'] = [
                dict(
                    timestamp = self.__get_phil_time__().strftime('%F %H:%M:%S'),
                    changes = 'set status to paid since running balance can cover the amount',
                    by = 'MQ First Month Invoice'
                )
            ]
        """

        doc_order['running_balance'] = '%0.2f' % running_balance

        #save order
        db_client_docs.save(doc_order)

        #return doc_id
        return doc_order['_id']


    def set_mysql_invoice_status(self, sid, status):
        """given the subcontractors_invoice_setup.id update its status
        """
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("""UPDATE subcontractors_invoice_setup
            SET status = %s where id = %s
            """, (status, sid))
        db.commit()

        #get leads_id from sid
        c.execute("""SELECT leads_id 
            FROM subcontractors_invoice_setup
            WHERE id = %s
            """, (sid))
        leads_id = c.fetchone()[0]

        #send mq message
        if status == 'paid':
            cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
                settings.PIKA_CRED['PASSWORD'])
            params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
                virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue='activate_client_suspended_staff_contracts', durable=True)
            channel.basic_publish(exchange='',
                          routing_key='activate_client_suspended_staff_contracts',
                          body='%s' % leads_id)
            connection.close()
            connection.socket.close()

        return 'ok'
        

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


if __name__ == '__main__':
    invoice = PrepaidInvoice()
##~    invoice.add_week_days(datetime.now(), 22)
##~    print invoice.create_invoice(1)
##~    invoice.send_email('37e8bb315ce1975bd43b2e6c1d00cb2e')
##~    invoice.send_email('b6d11eae32974a056ec87eab48163e06')
    invoice.send_email('734cebe6bba0b261683226703ccb4fe8')

##~    invoice.set_mysql_invoice_status(1, 'for invoicing')
##~    doc_id = invoice.create_invoice(1)
##~    invoice.send_email(doc_id)
##~    invoice.resend_email(1)

