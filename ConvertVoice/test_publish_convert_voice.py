#!/usr/bin/env python

import pika
import simplejson as json
import settings
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

def publish(userid, scale):
    """
    userid is from personal.userid
    scale is the scale from lame encoder
              n > 1: increase volume
              n = 1: no effect
              n < 1: reduce volume

    >>> publish(73584, 1)
    """
    parameters = pika.URLParameters(settings.PIKA_URL)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue='mp3_conversion', durable=True)

    json_message = dict(
        userid = userid,
        scale = scale,
        published_by = 'mq/ConvertVoice/test_publish_convert_voice.py'
        )

    json_message = json.dumps(json_message)
    channel.basic_publish(exchange='', routing_key='mp3_conversion', body=json_message)
    connection.close()


if __name__ == '__main__':
    import doctest
    doctest.testmod()
