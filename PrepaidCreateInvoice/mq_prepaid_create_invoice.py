#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-04-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on mq bindings

from PrepaidInvoice import PrepaidInvoice
import settings
import pika
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='prepaid_first_month_invoice', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] creating prepaid first month invoice %r" % (body, ))
    invoice = PrepaidInvoice()
    if re.match('resend', body):
        sid = int(re.split(' ', body)[1])
        invoice.resend_email(sid)
    else:
        doc_id = invoice.create_invoice(body)
        if doc_id == None:
            logging.info(" Failed creating prepaid firstmonth invoice %r" % (body, ))
            return
        doc_status = invoice.send_email(doc_id)
        if doc_status == 'new':   #set status
            invoice.set_mysql_invoice_status(body, 'awaiting payment')
        elif doc_status == 'paid':
            invoice.set_mysql_invoice_status(body, 'paid')

channel.basic_consume(callback, queue='prepaid_first_month_invoice', no_ack=True)

channel.start_consuming()
