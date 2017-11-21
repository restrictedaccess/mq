#!/usr/bin/env python
#   this mq is intended to run on production server 
#   where it has access to settings.VOICE_PATH for proper uploading

#   2013-05-07  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add redis timing to avoid subsequent conversion
#   2013-04-22  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   lessened verbosity of download error

import pika
import simplejson as json
import settings
from celery.execute import send_task
import ConvertVoice
import simplejson as json
import logging
import redis
SECONDS_LIMIT_SUBSEQUENT_CONVERSION = 10

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logging.info(' [*] Waiting for logs. To exit press CTRL+C')

parameters = pika.URLParameters(settings.PIKA_URL)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.queue_declare(queue='mp3_conversion', durable=True)


def callback(ch, method, properties, data):
    logging.info(" [x] %r" % (data, ))
    send_task('skype_messaging.notify_devs', ['ConvertVoice %s' % data])
    json_data = json.loads(data)
    userid = json_data.get('userid')
    scale = json_data.get('scale')
    if scale == None:
        scale = 1

    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    if r.get('mp3_conversion:%s' % userid) != None:  #key found ignore
        send_task('skype_messaging.notify_devs', ['Skipped ConvertVoice %s' % data])
        return

    filename = ConvertVoice.download(userid)
    if filename == None:
        send_task('skype_messaging.notify_devs', ['Failed ConvertVoice %s' % data])
        return
    ConvertVoice.media_to_mp3_ogg(userid, scale, filename)
    ConvertVoice.move_mp3_ogg(userid)
    ConvertVoice.update_voice_path(userid)
    send_task('skype_messaging.notify_devs', ['Finished ConvertVoice %s' % data])

    r.set('mp3_conversion:%s' % userid, data)
    r.expire('mp3_conversion:%s' % userid, SECONDS_LIMIT_SUBSEQUENT_CONVERSION)


channel.basic_consume(callback, queue='mp3_conversion', no_ack=True)
channel.start_consuming()

