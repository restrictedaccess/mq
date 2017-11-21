#   2012-05-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on mispelled recipients
#   2012-05-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   prevent connection timeout 

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

from sqlalchemy import create_engine
from sqlalchemy.sql import text

import re

import settings 
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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


def validateEmail(email):
	if len(email) > 7:
		if re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", email) != None:
			return True
	return False


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

        engine = create_engine(settings.MYSQL_DSN)
        conn = engine.connect()

        #get currency
        s = text("""SELECT sign from currency_lookup
            WHERE code = :code
            """)
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
        msg['couch_doc_id'] = doc_id

        #check acct_dept_email1 and acct_dept_email2 fields and other sorts using task GetEmails
        result = send_task("EmailSender.GetEmails", [doc['client_id']])
        email_recipients = result.get()
        msg['To'] = email_recipients['to']
        if email_recipients['cc'] != '':
            msg['Cc'] = email_recipients['cc']

        msg['From'] = 'noreply@remotestaff.com.au'

        subject = 'REMOTE STAFF PREPAID ORDER # %s' % doc['order_id']
        recipients = email_recipients['recipients']
        recipients.append('devs@remotestaff.com.au')
        recipients.append('accounts@remotestaff.com.au')
        recipients.append('orders@remotestaff.com.au')

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
        db_client_docs.save(doc)


class GetEmails(Task):
    def run(self, client_id):
        return_data = dict(
            recipients = [],
            to = '',
            cc = '',
        )
        engine = create_engine(settings.MYSQL_DSN)
        conn = engine.connect()
        sql = text("""SELECT address_to, default_email_field, cc_emails
            FROM leads_send_invoice_setting
            WHERE leads_id = :client_id
            """)
        client_data = conn.execute(sql, client_id=client_id).fetchone()

        # records found on leads_send_invoice_setting
        if client_data != None: 
            recipients = []
            if client_data.address_to == 'main_acct_holder':
                sql = text("""SELECT fname, lname, email, acct_dept_email1,
                    acct_dept_email2
                    FROM leads
                    WHERE id = :client_id
                    """)
                a = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'] = '"%s %s" <%s>' % (string.capitalize(a.fname), string.capitalize(a.lname), a.email)
                recipients.append(a.email)
            else:
                sql = text("""SELECT %s FROM leads
                    where id = :client_id
                    """ % client_data.address_to)
                result = conn.execute(sql, client_id=client_id).fetchone()
                return_data['to'] = result[client_data.address_to]
                recipients.append(result[client_data.address_to])

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
                    if validateEmail(email):
                        cc.append(email)
                        recipients.append(email)
                    else:
                        logging.info('Invalid leads.%s:%s for client_id:%s ' % (cc_email, email, client_id))

            return_data['recipients'] = recipients
            return_data['cc'] = string.join(cc, ',')

        else:
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
                if validateEmail(a.email):
                    recipients.append(a.email)
                else:
                    logging.info('Invalid leads.email:%s for client_id:%s ' % (a.email, client_id))
            if a.acct_dept_email1 not in ['', None]:
                if validateEmail(a.acct_dept_email1):
                    recipients.append(a.acct_dept_email1)
                else:
                    logging.info('Invalid leads.acct_dept_email1:%s for client_id:%s' % (a.acct_dept_email1, client_id))
            if a.acct_dept_email2 not in ['', None]:
                if validateEmail(a.acct_dept_email2):
                    recipients.append(a.acct_dept_email2)
                else:
                    logging.info('Invalid leads.acct_dept_email2:%s for client_id:%s' % (a.acct_dept_email2, client_id))

            return_data['recipients'] = recipients
            return_data['to'] = '"%s %s" <%s>' % (string.capitalize(a.fname), string.capitalize(a.lname), a.email)

        return return_data

if __name__ == '__main__':
    t = TopupOrder()
    t.run('67a76847c35dc74d0f60b63611373b60')
##~    e = GetEmails()
##~    pprint(e.run(11))
##~    pprint(e.run(6597))
##~    pprint(e.run(5439))
##~    pprint(e.run(7251))
##~    pprint(e.run(4896))
##~    pprint(e.run(7497))
##~    pprint(e.run(87497))
