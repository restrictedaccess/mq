#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added socket_timeout and updated exchange_type
import pika
import settings
import simplejson as json
from pprint import pprint, pformat
import skype_messaging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
    settings.PIKA_CRED['PASSWORD'])
params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
    virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred, socket_timeout=2)
connection = pika.BlockingConnection(params)

channel = connection.channel()
channel.exchange_declare(exchange='notify_devs', exchange_type='fanout')
result = channel.queue_declare(exclusive=True)

queue_name = result.method.queue

channel.queue_bind(exchange='notify_devs', queue=queue_name)


channel.exchange_declare(exchange='repo_push', exchange_type='fanout')
result_repo_push = channel.queue_declare(exclusive=True)

queue_name_repo_push = result_repo_push.method.queue

channel.queue_bind(exchange='repo_push', queue=queue_name_repo_push)


logging.info(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    logging.info(" [x] %r" % body)
    json_message = json.loads(body)
    subject = json_message['subject']
    message = json_message['message']

    message = '%s\n%s' % (subject, message)
    skype_messaging.send_message(message, group_chat='#locsunglao/$74ee209df36c0adc')


def callback_repo_push(ch, method, properties, body):
    logging.info(" [x] %r" % body)
    skype_messaging.send_message(body, group_chat='#locsunglao/$74ee209df36c0adc')


channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.basic_consume(callback_repo_push,
                      queue=queue_name_repo_push,
                      no_ack=True)

channel.start_consuming()
