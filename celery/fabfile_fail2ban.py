#!/usr/bin/env python
"""
used by fail2ban action when an ip is banned
executed via fabric
"""

#add to path the celeryconfig.py
import sys
sys.path.append("/home/remotestaff/mq/celery")

from fabric.api import lcd, local, env, task
from celery.execute import send_task

@task
def test(msg):
    send_task('skype_messaging.notify_skype_id', [msg,'locsunglao'])

@task
def ban(ip):
    send_task('skype_messaging.notify_devs', ["fail2ban Ban: %s" % ip])

@task
def unban(ip):
    send_task('skype_messaging.notify_devs', ["fail2ban Unban: %s" % ip])

