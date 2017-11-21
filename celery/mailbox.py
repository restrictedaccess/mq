#   2014-04-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add exception, used rackspace smtp if ses connection failes
#   2013-08-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated default mail recepient on test
#   2013-08-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   support for unicode text
#   2013-06-17  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added In-Reply-To and References email headers in preparation for email to ticket
#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added rate_limit on sending email
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-28  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   considered sender and reply_to field
#   2013-03-26  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
import couchdb
import re
import string
import sys
from pprint import pformat

from sqlalchemy.sql import text
import pika
from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)


@task(ignore_result=True, rate_limit='3/s')
def send(doc_id):
    """given the doc_id, send email
    """
    #couchdb settings
    couch_server = couchdb.Server(settings.COUCH_DSN)
    db = couch_server['mailbox']

    doc = db.get(doc_id)
    if doc == None:
        raise Exception('Failed to Send Invoice', '%s not found' % doc_id)

    msg = MIMEMultipart()

    #text part
    if doc.has_key('text'):
        if doc['text'] != None :
            text_message = string.strip(doc['text'])
            if text_message != '':
                part1 = MIMEText('%s' % text_message, 'plain', 'utf-8')
                part1.add_header('Content-Disposition', 'inline')
                msg.attach(part1)

    if doc.has_key('html'):
        if doc['html'] != None :
            html_message = string.strip(doc['html'])
            if html_message != '':
                part2 = MIMEText('%s' % html_message, 'html', 'UTF-8')
                part2.add_header('Content-Disposition', 'inline')
                msg.attach(part2)

    subject = doc['subject']
    if settings.DEBUG == True:
        subject = 'TEST %s' % subject
    msg['Subject'] = subject

    msg['From'] = doc['from']

    msg['To'] = string.join(doc['to'], ',')

    if doc.has_key('cc'):
        if doc['cc'] != None:
            msg['Cc'] = string.join(doc['cc'], ',')

    #attachments
    if doc.has_key('_attachments'):
        for attachment in doc['_attachments']:
            f = db.get_attachment(doc, attachment)
            data = f.read()
            file_attachment = MIMEApplication(data)
            file_attachment.add_header('Content-Disposition', 'attachment', filename=attachment)
            msg.attach(file_attachment)


    if settings.DEBUG == True:
        s = smtplib.SMTP(host = settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.starttls()
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )
    else:   #use SES on prod
        try:    #failed to establish with ses, use alternative
            s = smtplib.SMTP(host = settings.SMTP_CONFIG_SES['server'],
                port = settings.SMTP_CONFIG_SES['port'])
            s.starttls()
            s.login(settings.SMTP_CONFIG_SES['username'],
                settings.SMTP_CONFIG_SES['password']
                )
        except:
            send_task('notify_devs.send', ['FAILED TO CONNECTO TO SES SMTP SERVER : mailbox doc_id:%s, will retry using rackspace.' % doc_id, '%r' % [sys.exc_info()]])
            s = smtplib.SMTP(host = settings.SMTP_CONFIG['server'],
                port = settings.SMTP_CONFIG['port'])
            s.starttls()
            s.login(settings.SMTP_CONFIG['username'],
                settings.SMTP_CONFIG['password']
                )

    #collect recipients
    recipients = []
    if settings.DEBUG:
        recipients.append('devs@remotestaff.com.au')
    else:
        for email in doc['to']:
            recipients.append(email)

        if doc.has_key('cc'):
            if doc['cc'] != None:
                for email in doc['cc']:
                    recipients.append(email)

        if doc.has_key('bcc'):
            if doc['bcc'] != None:
                for email in doc['bcc']:
                    recipients.append(email)

    #add reply_to field
    reply_to = doc.get('reply_to')
    if reply_to not in (None, ''):
        msg['Reply-To'] = reply_to

    sender = doc.get('sender')
    if sender not in (None, ''):
        msg['Sender'] = sender

    #add In-Reply-To and References headers
    in_reply_to = doc.get('In-Reply-To')
    if in_reply_to != None:
        msg['In-Reply-To'] = in_reply_to

    references = doc.get('References')
    if references != None:
        msg['References'] = references


    failed_sending_ses = False
    try:
        s.sendmail(doc['from'], 
            recipients,
            msg.as_string())
    except:
        logging.exception("FAILED TO SEND couchdb mailbox VIA SES :%s" % doc_id)
        failed_sending_ses = True
        send_task('notify_devs.send', ['FAILED TO SEND couchdb mailbox VIA SES : %s, will retry using rackspace.' % doc_id, '%r' % [sys.exc_info()]])
    s.quit()

    if failed_sending_ses == True:  #try sending using rackspace
        s = smtplib.SMTP(host = settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.starttls()
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )

        try:
            s.sendmail(doc['from'], 
                recipients,
                msg.as_string())
        except:
            logging.exception("FAILED TO SEND couchdb mailbox VIA rackspace :%s" % doc_id)
            send_task('notify_devs.send', ['FAILED TO SEND couchdb mailbox VIA rackspace : %s. Please verify/re-send' % doc_id, '%r' % [sys.exc_info()]])
        s.quit()

    #add history that the document was send
    history = doc.get('history')
    if history == None:
        history = []

    history.append('%s sent by celery process, mailbox.send' % get_ph_time())
    doc['history'] = history
    doc['sent'] = True
    db.save(doc)

    return


def run_tests():
    """
    >>> send('fail_doc')
    Traceback (most recent call last):
        ...
    Exception: ('Failed to Send Invoice', 'fail_doc not found')

##~    >>> send('sample_email_doc')
    >>> send('sample_email_doc_with_in_reply_to')

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
