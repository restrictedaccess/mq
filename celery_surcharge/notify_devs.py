#   2013-07-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   trim skype_message to 300 characters to prevent unresponsive skype client 
#   2013-06-19  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add group_chat parameter to send directly to each devs 
#   2013-04-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed fanout type, will disable mq/NotifyDevsViaSkype since publishing to 10.9.8.7 is unreliable
#   2013-04-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated parameter type
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-01-31  lawrence sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add publisher to notify devs
#   -   add sending to rackspace if for some reason rackspace fails due to rate limits
#   2012-11-21  lawrence sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task to send email notification to devs

import settings
from celery.task import task
import couchdb

#import skype_messaging

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import simplejson as json

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from pytz import timezone
from datetime import datetime


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


@task(ignore_result=True)
def send(subject, message, group_chat=False):

    #skype_message = '%s\n%s' % (subject, message)
    
    #if group_chat == True:
    #    skype_messaging.notify_devs(skype_message[:300])
    #else:
    #    skype_messaging.notify_each_devs(skype_message[:300])
    
    #send email via mailbox
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['mailbox']

    doc = dict(
        sent = False,		
        created = get_ph_time(as_array=True),
        generated_by = 'celery notify_devs.send',
        text = message,
        subject = 'NOTIFY DEVS %s' % subject,
        to = ['devs@remotestaff.com.au']
    )
    doc['from'] = "NOTIFY DEVS<noreply@remotestaff.com.au>"
    db.save(doc)


def run_tests():
    """
    >>> send('TEST 123', 'THE QUICK BROWN FOX', True)

    >>> send('TEST 123', 'THE QUICK BROWN FOX')

    """

if __name__ == '__main__':
    import doctest
    doctest.testmod()
