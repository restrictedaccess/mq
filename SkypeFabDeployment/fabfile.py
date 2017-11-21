#!/usr/bin/env python

import redis
from fabric.api import lcd, local, env, task
import string
import re
import settings
import random
import os
import tarfile
import logging
from pytz import timezone
from datetime import datetime
from celery.execute import send_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

@task
def status(repo, changeset):
    with lcd('%s/%s' % (settings.REPO_DIR, repo)):
        local("hg log -r %s" % changeset)
        local("hg status --change %s" % changeset)


@task
def pull(repo):
    with lcd('%s/%s' % (settings.REPO_DIR, repo)):
        local("hg pull")
        local("hg update")
        local("hg log -r tip")
        local("hg status --change tip")


def generate_key():
    key = random.sample(string.letters + string.digits, 18)
    key = string.join(key, '')
    return key


@task
def deploy(repo, changeset, key=None):
    with lcd('%s/%s' % (settings.REPO_DIR, repo)):
        local("hg pull")
        r = redis.StrictRedis()
        if key == None:
            local("hg update -r %s" % changeset)
            data = local("hg log -r %s" % changeset, capture=True)
            data += "\nFiles :\n"
            x = local("hg status --change %s" % changeset, capture=True)
            lines = string.split(x, '\n')
            
            for line in lines:
                [status, file_name] = string.split(line, ' ', 1)

                #exclude config files
                if file_name in settings.CONFIG_FILES[repo]:
                    continue

                #exclude pyjamas
                if repo == 'portal':
                    if re.match('^pyjamas/', file_name) != None:
                        continue

                if status in ('A', 'M'):
                    data += '%s\n' % file_name

            new_key = generate_key()
            data += "\nTo deploy:\n@fab %s %s deploy:repo=%s,changeset=%s,key=%s\n" % (settings.REPO_USERNAME, settings.REPO_HOST_NAME, repo, changeset, new_key)
            data += "You have %s seconds before the key expires.\n" % settings.SECONDS_KEY_EXPIRE
            logging.info(data)
            send_task('skype_messaging.notify_devs', [data])
            r.set('fab_deploy:%s' % new_key, changeset)
            r.expire('fab_deploy:%s' % new_key, settings.SECONDS_KEY_EXPIRE)
        
        else:
            #verify key
            recorded_changeset = r.get('fab_deploy:%s' % key)
            if recorded_changeset != changeset:
                logging.error("key probably expired")
                send_task('skype_messaging.notify_devs', ['key probably expired'])
            else:
                #delete key
                r.delete('fab_deploy%s' % key)
                deploy_confirmed(repo, changeset)


def deploy_confirmed(repo, changeset):
    """
    >>> deploy_confirmed('portal', 558)
    [localhost] local: hg update -r 558
    [localhost] local: hg status --change 558
    [localhost] local: rsync -R recruiter/css/style.css /home/oliver/tmp2/portal/
    [localhost] local: rsync -R recruiter/js/staff_information.js /home/oliver/tmp2/portal/

    >>> deploy_confirmed('portal', 2866)
    [localhost] local: hg update -r 2866
    [localhost] local: hg status --change 2866
    [localhost] local: rsync -R celery/fabfile_fail2ban.py /home/oliver/tmp2/mq/

    >>> deploy_confirmed('portal', 2765)
    [localhost] local: hg update -r 2765
    [localhost] local: hg status --change 2765
    [localhost] local: rsync -R client_topup/views.py /home/oliver/tmp2/django/
    [localhost] local: rsync -R client_topup_prepaid/views.py /home/oliver/tmp2/django/

    >>> deploy_confirmed('home_pages', 455)
    [localhost] local: hg update -r 455
    [localhost] local: hg status --change 455
    [localhost] local: rsync -R livechat/chatlogin.php /home/oliver/tmp2/home_pages/
    [localhost] local: rsync -R livechat/getAdmin.php /home/oliver/tmp2/home_pages/
    [localhost] local: rsync -R livechat/updateRecord.php /home/oliver/tmp2/home_pages/
    [localhost] local: rsync -R livechat/users_status.php /home/oliver/tmp2/home_pages/
    [localhost] local: rsync -R webchat.php /home/oliver/tmp2/home_pages/

    >>> deploy_confirmed('home_pages', 1605)    #with config file
    [localhost] local: hg update -r 1605
    [localhost] local: hg status --change 1605
    [localhost] local: rsync -R about/js/register.js /home/oliver/tmp2/home_pages/


    """
    with lcd('%s/%s' % (settings.REPO_DIR, repo)):
        local("hg update -r %s" % changeset)
        x = local("hg status --change %s" % changeset, capture=True)
        lines = string.split(x, '\n')

        #backup files
        now = get_ph_time().strftime('%F_%H-%M-%S')
        tar = tarfile.open('%s/%s.%s.%s.tbz' % (settings.TAR_FILE_PATH, repo, changeset, now), 'w:bz2')

        #loop over files
        for line in lines:
            [status, file_name] = string.split(line, ' ', 1)

            flag_sync_django_portal = False
            if status in ('A', 'M'):
                path = settings.SYNC_PATH[repo]['__default__']
                for k in settings.SYNC_PATH[repo].keys():
                    if re.match('^%s' % k, file_name):
                        path = settings.SYNC_PATH[repo][k]
                        cwd = k

                if path == None:
                    continue

                if path == settings.SYNC_PATH[repo]['__default__']:
                    cwd2 = '%s/%s' % (settings.REPO_DIR, repo)
                    file_name2 = file_name
                else:
                    cwd2 = '%s/%s/%s' % (settings.REPO_DIR, repo, cwd)
                    file_name2 = re.sub('^%s' % cwd, '', file_name)

                with lcd(cwd2):
                    tar_file = '%s/%s' % (path, file_name2)
                    #check if file exists
                    if os.path.isfile(tar_file):
                        tar.add(tar_file)

                    #exclude config files
                    if file_name not in settings.CONFIG_FILES[repo]:    #not file_name2 because of the config
                        local("rsync -R '%s' %s/" % (file_name2, path))
                    else:   #notify devs on config file change
                        notify = 'config file %s has changes' % tar_file
                        send_task('skype_messaging.notify_devs', [notify])

        tar.close()


@task
def restart_django_portal():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart django-portal:*')


@task
def restart_django_adhoc():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart django-adhoc:*')


@task
def restart_django_remotestaff():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart django-remotestaff:*')


@task
def restart_django_home_pages():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart django_home_pages:*')


@task
def restart_django_jobopenings():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart django_jobopenings:*')


@task
def supervisorctrl_status():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl status')


@task
def restart_celery():
    with lcd(settings.SUPERVISOR_PATH):
        local('supervisorctl restart celery')


def get_ph_time(as_array=False):
    """returns a philippines datetime
    >>> a = get_ph_time()
    >>> logging.info(a)
    >>> logging.info(a.strftime('%F_%H-%M-%S'))
    >>> a = get_ph_time(as_array=True)
    >>> logging.info(a)
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    if as_array == True:
        return [now.year, now.month, now.day, now.hour, now.minute, now.second]
    else:
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


@task
def sync_www_realestate_com_ph():
    with lcd('%s/%s' % (settings.REPO_DIR, 'www_realestate_com_ph')):
        local("hg pull")
        local("hg update default")
        local("rsync -rvc --delete * /home/remotestaff/www.realestate.com.ph/html/")


if __name__ == '__main__':
    import doctest
    doctest.testmod()

