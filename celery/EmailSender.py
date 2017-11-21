#   2013-07-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   replaced validateEmail with python package validate_email
#   2013-06-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task for retrieving address_to
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix main_acct_holder (default_email_field)
#   -   added name to address_to
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2012-11-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added function to get CSRO admin details given the client_id
#   2012-10-20  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed accounts
#   2012-10-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on retrieving default_email_field
#   2012-09-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used default_email_field instead of address_to for the GetEmails
#   2012-09-25  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used default_email_field instead of address_to for the GetPlainEmails
#   2012-09-19  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a task to query plain emails of client
#   2012-06-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a task to query staffs csro email depending on subcontracts and lead
#   2012-06-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   verbosely added cc settings on invoices sent
#   -   updated leads_send_invoice_setting logic
#   2012-05-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added a function to query the assigned csro email
#   -   send an email alert if the assigned csro has been removed

from celery.task import task, Task
from celery.execute import send_task

from celery import Celery
import sc_celeryconfig


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

from persistent_mysql_connection import engine
from sqlalchemy.sql import text

import re

import settings 
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from validate_email import validate_email

couch_server = couchdb.Server(settings.COUCH_DSN)
db_client_docs = couch_server['client_docs']

utc = timezone('UTC')
phtz = timezone('Asia/Manila')


def get_ph_time():
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


@task(ignore_result=True)
class TopupOrder(Task):
    def run(self, doc_id):
        """given the doc_id, send email
        """
        logging.info('Sending topup order %s ' % doc_id)
        couch_server = couchdb.Server(settings.COUCH_DSN)
        db_client_docs = couch_server['client_docs']
        doc = db_client_docs.get(doc_id)

        #get the template
        t = Template(file="templates/client_topup_invoice_created.tmpl")

        #assign variables to the template file
        t.client_fname = doc['client_fname']
        t.client_lname = doc['client_lname']

        #get currency
        s = text("""SELECT sign from currency_lookup
            WHERE code = :code
            """)
        conn = engine.connect()
        r  = conn.execute(s, code=doc['currency']).fetchone()
        t.currency_sign = r.sign

        #get client details
        s = text("""SELECT company_name, company_address, acct_dept_email1, acct_dept_email2
            FROM leads
            WHERE id = :client_id
            """)
        client_data = conn.execute(s, client_id=doc['client_id']).fetchone()
        t.client_data = client_data

        items_converted_date = []
        items = doc['items']

        for item in items:
            a = item.copy()
            if a.has_key('start_date'):
                b = a['start_date']
                a['start_date'] = date(b[0], b[1], b[2])
            else:
                a['start_date'] = None

            if a.has_key('end_date'):
                c = a['end_date']
                a['end_date'] = date(c[0], c[1], c[2])
            else:
                a['end_date'] = None

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
        t.status = doc['status']

        html_message = '%s' % t

        msg = MIMEMultipart()
        part1 = MIMEText('%s' % html_message, 'html')
        part1.add_header('Content-Disposition', 'inline')
        msg.attach(part1)

        #check acct_dept_email1 and acct_dept_email2 fields and other sorts using task GetEmails
        
        if settings.DEBUG:
            result = send_task("EmailSender.GetEmails", [doc['client_id']])
        else:
            celery = Celery()
            celery.config_from_object(sc_celeryconfig)
            result = celery.send_task("EmailSender.GetEmails", [doc['client_id']])
            
        email_recipients = result.get()
        msg['To'] = email_recipients['to']
        if email_recipients['cc'] != '':
            msg['Cc'] = email_recipients['cc']

        msg['From'] = 'accounts@remotestaff.com.au'
        msg['Reply-To'] = 'accounts@remotestaff.com.au'

        subject = 'REMOTE STAFF PREPAID ORDER # %s' % doc['order_id']
        recipients = email_recipients['recipients']
        recipients.append('devs@remotestaff.com.au')
        recipients.append('orders@remotestaff.com.au')

        if settings.DEBUG:
            subject = 'TEST %s' % subject
            recipients = [settings.EMAIL_ALERT]

        msg['Subject'] = subject


        s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
            port = settings.MAILGUN_CONFIG['port'])
        s.starttls()
        s.login(settings.MAILGUN_CONFIG['username'],
            settings.MAILGUN_CONFIG['password']
            )

        s.sendmail('noreply@remotestaff.com.au', 
            recipients,
            msg.as_string())
        s.quit()

        #store the email for archiving purposes
        history = []
        history.append(dict(
            timestamp = get_ph_time().strftime('%F %H:%M:%S'),
            changes = 'Order created',
            by = 'TopupOrder celery task'
        ))
        doc_order_archive = dict(
            type = 'order archive',
            email_recipients = email_recipients,
            history = history,
            client_id = int(doc['client_id']),
            subject = 'REMOTE STAFF PREPAID ORDER # %s' % doc['order_id'],
        )
        db_client_docs.save(doc_order_archive)
        #attach message
        db_client_docs.put_attachment(doc_order_archive, html_message, 'message.html')

        #add history that the document was sent
        doc = db_client_docs.get(doc_id)
        if doc.has_key('history') == False:
            history = []
        else:
            history = doc['history']

        history.append(dict(
            timestamp = get_ph_time().strftime('%F %H:%M:%S'),
            changes = 'Email sent to %s.' % string.join(email_recipients['recipients'], ','),
            by = 'TopupOrder celery task'
        ))

        doc['history'] = history
        doc['mongo_synced'] = False		
        db_client_docs.save(doc)
        conn.close()


class GetEmails(Task):
    def run(self, client_id):
        """
        >>> r = GetEmails()
        >>> r.run(8745)

        """
        return_data = dict(
            recipients = [],
            to = '',
            cc = '',
        )
        sql = text("""SELECT address_to, default_email_field, cc_emails
            FROM leads_send_invoice_setting
            WHERE leads_id = :client_id
            """)
        conn = engine.connect()
        client_data = conn.execute(sql, client_id=client_id).fetchone()

        # records found on leads_send_invoice_setting
        if client_data != None: 
            recipients = []
            if client_data.address_to == 'main_acct_holder':
                sql = text("""SELECT fname, lname,
                    %s AS email
                    FROM leads
                    WHERE id = :client_id
                    """ % client_data.default_email_field)

                a = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'] = '"%s %s" <%s>' % (string.capitalize(a.fname), string.capitalize(a.lname), a.email)
                recipients.append(a.email)
            else:
                sql = text("""SELECT * FROM leads
                    where id = :client_id
                    """)
                result = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'] = '"%s" <%s>' % (result[client_data.address_to], result[client_data.default_email_field])
                recipients.append(result[client_data.default_email_field])

            #process cc_emails
            cc = []
            cc_emails = client_data.cc_emails
            if cc_emails not in ['', None]:
                cc_emails = string.split(cc_emails, ',')
                for cc_email in cc_emails:
                    sql = text("""SELECT %s FROM leads
                        where id = :client_id
                        """ % cc_email)
                    b = conn.execute(sql, client_id=client_id).fetchone()
                    email = b[0]
                    if validate_email(email):
                        cc.append(email)
                        recipients.append(email)
                    else:
                        logging.info('Invalid leads.%s:%s for client_id:%s ' % (cc_email, email, client_id))

            return_data['recipients'] = recipients
            return_data['cc'] = string.join(cc, ',')

        else:
            cc =[]
            sql = text("""SELECT fname, lname, email, acct_dept_email1,
                acct_dept_email2
                FROM leads
                WHERE id = :client_id
                """)
            a = conn.execute(sql, client_id=client_id).fetchone()
            if a == None:
                return_data['recipients'] = ['devs@remotestaff.com.au']
                return_data['to'] = 'Invalid client_id:%s' % client_id
                return return_data
            recipients = []
            if a.email not in ['', None]:
                if validate_email(a.email):
                    recipients.append(a.email)
                else:
                    logging.info('Invalid leads.email:%s for client_id:%s ' % (a.email, client_id))
            if a.acct_dept_email1 not in ['', None]:
                if validate_email(a.acct_dept_email1):
                    recipients.append(a.acct_dept_email1)
                    cc.append(a.acct_dept_email1)
                else:
                    logging.info('Invalid leads.acct_dept_email1:%s for client_id:%s' % (a.acct_dept_email1, client_id))
            if a.acct_dept_email2 not in ['', None]:
                if validate_email(a.acct_dept_email2):
                    recipients.append(a.acct_dept_email2)
                    cc.append(a.acct_dept_email2)
                else:
                    logging.info('Invalid leads.acct_dept_email2:%s for client_id:%s' % (a.acct_dept_email2, client_id))

            return_data['recipients'] = recipients
            return_data['to'] = '"%s %s" <%s>' % (string.capitalize(a.fname), string.capitalize(a.lname), a.email)
            return_data['cc'] = string.join(cc, ',')

        conn.close()
        return return_data


class GetAddressTo(Task):
    def run(self, client_id):
        address_to =  ''
        sql = text("""SELECT address_to, default_email_field, cc_emails
            FROM leads_send_invoice_setting
            WHERE leads_id = :client_id
            """)
        conn = engine.connect()
        client_data = conn.execute(sql, client_id=client_id).fetchone()

        # records found on leads_send_invoice_setting
        if client_data != None: 
            if client_data.address_to == 'main_acct_holder':
                sql = text("""SELECT fname, lname,
                    %s AS email
                    FROM leads
                    WHERE id = :client_id
                    """ % client_data.default_email_field)

                a = conn.execute(sql, client_id=client_id).fetchone()
                address_to = '%s %s' % (string.capitalize(a.fname), string.capitalize(a.lname))
            else:
                sql = text("""SELECT * FROM leads
                    where id = :client_id
                    """)
                result = conn.execute(sql, client_id=client_id).fetchone()
                address_to = '%s' % (result[client_data.address_to])

        else:
            sql = text("""SELECT fname, lname, email, acct_dept_email1,
                acct_dept_email2
                FROM leads
                WHERE id = :client_id
                """)
            a = conn.execute(sql, client_id=client_id).fetchone()
            if a == None:
                address_to = 'Invalid client_id:%s' % client_id

            address_to = '%s %s' % (string.capitalize(a.fname), string.capitalize(a.lname))

        conn.close()
        return address_to


class GetCsroEmail(Task):
    def run(self, client_id):
        """given the client_id, return the admin.admin_email
        """
        admin_email = None

        sql = text("""SELECT a.admin_id, a.admin_email, a.status FROM leads AS l
            JOIN admin AS a
            ON l.csro_id = a.admin_id
            WHERE l.id = :client_id
            """)
        conn = engine.connect()
        admin_data = conn.execute(sql, client_id=client_id).fetchone()

        if admin_data != None:
            admin_id, admin_email, admin_status = admin_data
            if admin_status == 'REMOVED':
                #cough out to log
                admin_status_message = 'Please fix csro assignment for leads.id # %s\ncsro_id:%s <%s> status has been REMOVED.' % (client_id, admin_id, admin_email)
                logging.info(admin_status_message)
                
                #send email
                msg = MIMEText(admin_status_message)
                s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
                    port = settings.MAILGUN_CONFIG['port'])
                s.starttls()
                s.login(settings.MAILGUN_CONFIG['username'],
                    settings.MAILGUN_CONFIG['password']
                    )
                recipients = ['devs@remotestaff.com.au', 'csro@remotestaff.com.au', 'admin@remotestaff.com.au']
                msg['Subject'] = 'CSRO Assignment Alert'

                if settings.DEBUG:
                    recipients = ['devs@remotestaff.com.au']
                    msg['Subject'] = 'TEST CSRO Assignment Alert'

                msg['To'] = string.join(recipients, ',')

                s.sendmail('noreply@remotestaff.com.au', 
                    recipients,
                    msg.as_string())
                s.quit()
                admin_email = None

        conn.close()
        return admin_email


@task(ignore_result=True)
def CSROAssignmentAlert(subject, message):
    if settings.DEBUG:
        subject = 'TEST %s' % subject
    logging.info('%s : %s' %  (subject, message))

    #send email
    msg = MIMEText(message)
    s = smtplib.SMTP(host = settings.MAILGUN_CONFIG['server'],
        port = settings.MAILGUN_CONFIG['port'])
    s.starttls()
    s.login(settings.MAILGUN_CONFIG['username'],
        settings.MAILGUN_CONFIG['password']
        )
    recipients = ['devs@remotestaff.com.au', 'csro@remotestaff.com.au', 'admin@remotestaff.com.au']
    msg['Subject'] = subject

    if settings.DEBUG:
        recipients = ['devs@remotestaff.com.au']

    msg['To'] = string.join(recipients, ',')

    s.sendmail('noreply@remotestaff.com.au', 
        recipients,
        msg.as_string())
    s.quit()


class GetCsroDetails(Task):
    def run(self, client_id):
        """given the client_id, return the admin
        returns None if not found
        """
        admin_data = None

        sql = text("""SELECT a.admin_id, a.userid, a.admin_fname, a.admin_lname,
            a.admin_email, a.status,
            a.signature_notes, a.signature_contact_nos, a.signature_company, a.signature_websites
            FROM leads AS l
            JOIN admin AS a
            ON l.csro_id = a.admin_id
            WHERE l.id = :client_id
            """)
        conn = engine.connect()
        admin_data = conn.execute(sql, client_id=client_id).fetchone()

        if admin_data != None:
            if admin_data.status == 'REMOVED':
                #cough out to log
                message = 'Please fix csro assignment for leads.id # %s\ncsro_id:%s <%s> status has been REMOVED.' % (client_id, admin_data.admin_id, admin_data.admin_email)
                CSROAssignmentAlert.delay('CSRO Assignment Alert', message)
        else:
                message = 'Please fix csro assignment for leads.id # %s\nNo CSRO assigned.' % (client_id)
                CSROAssignmentAlert.delay('CSRO Assignment Alert', message)

        conn.close()
        return admin_data


class GetCsroEmailsUserId(Task):
    def run(self, userid):
        """given the userid, return a list of admin emails
        """
        admin_emails = []

        sql = text("""
            SELECT DISTINCT(leads_id)
            FROM subcontractors
            WHERE userid = :userid
            AND STATUS = 'active'
        """)
        conn = engine.connect()
        result = conn.execute(sql, userid=userid).fetchall()
        csro_email = GetCsroEmail()
        for r in result:
            admin_email = csro_email.run(r[0])
            if admin_email != None:
                admin_emails.append(admin_email)

        conn.close()
        return admin_emails


class GetPlainEmails(Task):
    """returns a list of plain emails
    """
    def run(self, client_id):
        return_data = dict(
            to = [],
            cc = [],
        )
        sql = text("""SELECT address_to, default_email_field, cc_emails
            FROM leads_send_invoice_setting
            WHERE leads_id = :client_id
            """)
        conn = engine.connect()
        client_data = conn.execute(sql, client_id=client_id).fetchone()

        # records found on leads_send_invoice_setting
        if client_data != None: 
            if client_data.address_to == 'main_acct_holder':
                if client_data.default_email_field == 'email':
                    sql = text("""SELECT fname, lname, 
                        email
                        FROM leads
                        WHERE id = :client_id
                        """)
                elif client_data.default_email_field == 'acct_dept_email1':
                    sql = text("""SELECT fname, lname, 
                        acct_dept_email1 as email
                        FROM leads
                        WHERE id = :client_id
                        """)
                else:
                    sql = text("""SELECT fname, lname,
                        %s as email
                        FROM leads
                        WHERE id = :client_id
                        """ % client_data.default_email_field)
                a = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'].append(string.strip(a.email))
            else:
                sql = text("""SELECT %s FROM leads
                    where id = :client_id
                    """ % client_data.default_email_field)
                result = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'].append(result[client_data.default_email_field])

            #process cc_emails
            cc = []
            cc_emails = client_data.cc_emails
            if cc_emails not in ['', None]:
                cc_emails = string.split(cc_emails, ',')
                for cc_email in cc_emails:
                    sql = text("""SELECT %s FROM leads
                        where id = :client_id
                        """ % cc_email)
                    b = conn.execute(sql, client_id=client_id).fetchone()
                    email = b[0]
                    if validate_email(email):
                        cc.append(string.strip(email))
                    else:
                        logging.info('Invalid leads.%s:%s for client_id:%s ' % (cc_email, email, client_id))

            return_data['cc'] = cc

        else:
            cc =[]
            sql = text("""SELECT fname, lname, email, acct_dept_email1,
                acct_dept_email2
                FROM leads
                WHERE id = :client_id
                """)
            a = conn.execute(sql, client_id=client_id).fetchone()
            if a == None:
                return_data['to'].append('devs@remotestaff.com.au')
                return_data['to'].append('Invalid client_id:%s' % client_id)
                return return_data

            to = []
            if a.email not in ['', None]:
                if validate_email(a.email):
                    to.append(string.strip(a.email))
                else:
                    logging.info('Invalid leads.email:%s for client_id:%s ' % (a.email, client_id))

            if a.acct_dept_email1 not in ['', None]:
                if validate_email(a.acct_dept_email1):
                    cc.append(string.strip(a.acct_dept_email1))
                else:
                    logging.info('Invalid leads.acct_dept_email1:%s for client_id:%s' % (a.acct_dept_email1, client_id))
            if a.acct_dept_email2 not in ['', None]:
                if validate_email(a.acct_dept_email2):
                    cc.append(string.strip(a.acct_dept_email2))
                else:
                    logging.info('Invalid leads.acct_dept_email2:%s for client_id:%s' % (a.acct_dept_email2, client_id))

            return_data['to'] = to
            return_data['cc'] = cc

        conn.close()
        return return_data



if __name__ == '__main__':
    import doctest
    doctest.testmod()
##~    t = TopupOrder()
##~    t.run('8bbcd728fee6e329bb1a718d386f291c')
##~    e = GetEmails()
##~    e = GetPlainEmails()
##~    pprint(e.run(2954))     #no assigned csro
##~    pprint(e.run(6597))
##~    pprint(e.run(5439))
##~    pprint(e.run(7251))
##~    pprint(e.run(4896))
##~    pprint(e.run(7497))
##~    pprint(e.run(87497))
##~    pprint(e.run(11))

#    csro_email = GetCsroEmail()
#    pprint(csro_email.run(7557))
#    pprint(csro_email.run(5717))
#    pprint(csro_email.run(7734))
#    pprint(csro_email.run(11))
##~    csro_emails = GetCsroEmailsUserId()
##~    pprint(csro_emails.run(69))
##~    pprint(csro_emails.run(4229))

##~    send_task("EmailSender.GetCsroDetails", (2954,))    #no assigned csro
##~    data = send_task("EmailSender.GetCsroDetails", (6597,))    #no assigned csro
##~    admin = data.get()
##~    logging.info(admin.admin_fname)
