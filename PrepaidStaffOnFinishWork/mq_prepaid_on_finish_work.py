#!/usr/bin/env python
#   2012-08-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   bugfix on decimal conversion
#   2012-08-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added exception for weekly invoicing for daniel cowart
#   2012-07-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   send autorespoder to csro when load goes below 5 days, workflow 2702, item 4
#   2012-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   issue invoice 5 days worth of load
#   -   re-send on 2 days worth of load
#   2012-03-28  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   issue invoice 2 days worth of load
#   -   suspend contracts when load is zero

from PrepaidStaffOnFinishWork import PrepaidOnFinishWork 
import settings
import pika
from decimal import Decimal
import logging

from celery.execute import send_task

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')


cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='prepaid_on_finish_work', durable=True)

channel_suspend_staff = connection.channel()
channel_suspend_staff.queue_declare(queue='suspended', durable=True)

logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, doc_id):
    logging.info(" [x] %r" % (doc_id, ))
    prepaid = PrepaidOnFinishWork()
    doc_order = prepaid.add_worked_timerecord(doc_id)
    if doc_order != None:
        running_balance = Decimal(doc_order['running_balance'])
        client_id = doc_order['client_id']
        clients_daily_rate = prepaid.get_clients_daily_rate(client_id)

        #add exception for client Daniel Cowart #5427
        if client_id == 5427:
            logging.info('daniel cowart #5427 staff finished worked')
            if running_balance <= (Decimal('2.0') * clients_daily_rate):  #special 2 day notification
                doc_invoice = prepaid.has_open_invoice(client_id, settings.DAYS_INVOICE_OPEN)
                if doc_invoice == None:
                    doc_id_invoice = prepaid.create_invoice(client_id, 5)   #valid just for 5 days or 1 wk
                    if doc_id_invoice == None:
                        logging.info("error failed to create, doc_id_invoice is None")
                        return
                    logging.info("created invoice %s" % doc_id_invoice)

                    #celery task to send autoresponder to csro
                    send_task("PrepaidCSROAutoResponders.ClientRunningLowOnCredit", [doc_id_invoice])

                    if doc_id_invoice != None:
                        prepaid.send_email(doc_id_invoice)

                #check if we need to suspend client
                if running_balance <= Decimal(0):
                    logging.info("suspending daniel cowart client_id %s" % client_id)
                    channel_suspend_staff.basic_publish(exchange='',
                        routing_key='suspended',
                        body='%s' % client_id)

        else:
            if running_balance <= (Decimal(settings.DAYS_BEFORE_INVOICE_ISSUE) * clients_daily_rate):
                #issue invoice
                doc_invoice = prepaid.has_open_invoice(client_id, settings.DAYS_INVOICE_OPEN)
                if doc_invoice == None:
                    doc_id_invoice = prepaid.create_invoice(client_id)
                    if doc_id_invoice == None:
                        logging.info("error failed to create, doc_id_invoice is None")
                        return
                    logging.info("created invoice %s" % doc_id_invoice)

                    #celery task to send autoresponder to csro
                    send_task("PrepaidCSROAutoResponders.ClientRunningLowOnCredit", [doc_id_invoice])

                    if doc_id_invoice != None:
                        prepaid.send_email(doc_id_invoice)
                else:
                    #check if we need to send the second day notice
                    logging.info('client_id %s already has an open invoice checking for second_day_notice' % client_id)
                    if running_balance <= (Decimal(settings.DAYS_BEFORE_SECOND_NOTIFICATION) * clients_daily_rate):
                        if doc_invoice.has_key('second_day_notice') == False:
                            logging.info('setting second_day_notice for %s' % doc_invoice['_id'])

                            #celery task to send autoresponder to csro
                            send_task("PrepaidCSROAutoResponders.ClientRunningLowOnCredit", [doc_invoice['_id']])

                            prepaid.set_second_day_notice(doc_invoice['_id'])
                            prepaid.send_email(doc_invoice['_id'])

                #check if we need to suspend client
                if running_balance <= Decimal(0):
                    logging.info("suspending client_id %s" % client_id)
                    channel_suspend_staff.basic_publish(exchange='',
                        routing_key='suspended',
                        body='%s' % client_id)

channel.basic_consume(callback, queue='prepaid_on_finish_work', no_ack=True)

channel.start_consuming()
