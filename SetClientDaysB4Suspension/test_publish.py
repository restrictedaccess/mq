#!/usr/bin/env python

import pika
import simplejson as json
import settings
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def publish(leads_id):
    """
    >>> publish(7183)

    """
    parameters = pika.URLParameters(settings.PIKA_URL)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue='set_client_days_b4_suspension', durable=True)

    json_message = dict(
        leads_id = leads_id,
        publisher = 'mq/SetClientDaysB4Suspension/test_publish.py'
        )

    json_message = json.dumps(json_message)
    channel.basic_publish(exchange='', routing_key='set_client_days_b4_suspension', body=json_message)
    connection.close()


if __name__ == '__main__':
    import doctest
    doctest.testmod()
