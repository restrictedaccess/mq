#!/usr/bin/env python

import pika
import settings
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def publish(userid, scale):
    """
    userid is from personal.userid
    scale is the scale from lame encoder
              n > 1: increase volume
              n = 1: no effect
              n < 1: reduce volume

    >>> publish(71797, 4)
    >>> #publish(72260, 1)
    """
    parameters = pika.URLParameters(settings.PIKA_URL)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue='image_replication', durable=True)

    f = open("DEVS.jpg", "rb")
    s = f.read()
    f.close()

    fields = dict(
        filetype = 'screenshot',
        filename = 'DEVS.jpg',
        week = 1,
        year = 2013
    )

    for i in range(500):
        channel.basic_publish(exchange='', routing_key='image_replication', body=s, properties = pika.BasicProperties(headers=fields))
    connection.close()


if __name__ == '__main__':
    import doctest
    doctest.testmod()
