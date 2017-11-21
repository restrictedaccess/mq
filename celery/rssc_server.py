#   2013-09-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add rate_limit on some task
#   2013-08-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task to check active subcontractor records
#   2013-05-29  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   disabled checking if connection is wireless
#   2013-05-22  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the admin.skype_id instead of looking up to personal table
#   2013-05-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added retry limit on saving platform
#   2013-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix save_retry_disconnect when staff disconnects from rssc
#   2013-04-08  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   updated parameter type
#   2013-04-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added time_limit, added save retry limit
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-10  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add publish to mq for disconnected staff
#   2013-03-07  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed skype logging
#   2013-03-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   ignored result on recording connections to avoid lock ups
#   -   retry saving platform
#   -   dont flood dev group on rssc connection errors
#   2013-02-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added traceback and sys imports
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   -   retry executing failed disconnection
#   2013-01-31  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used the mysql persistent connection
#   2013-01-09 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added retry to silence sending of failed whois
#   2012-12-26 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add task for whois, wireless connection setting
#   2012-12-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   transferred logging of connections to celery for hooking up wireless connection detection
#   2012-12-11 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task for retrieving activity notes

import settings
import couchdb
from celery.task import task
from datetime import datetime, timedelta
from pytz import timezone
from pprint import pprint, pformat
from WhoIs import NICClient
import string
import simplejson as json

from persistent_mysql_connection import engine
from sqlalchemy.sql import text 

import pika

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import re
from celery.execute import send_task

import sys, traceback
import redis

SAVE_RETRY = 5

s = couchdb.Server(settings.COUCH_DSN)
db = s['rssc']

def compare_working_details(doc_userid):
    if doc_userid.has_key('working_details'):
        if doc_userid['working_details'].has_key('intervals'):
            subcontractors_id = doc_userid['working_details']['subcontract']
            doc_subcontract = db.get('subcon-%s' % subcontractors_id)
            doc_subcontract_intervals = doc_subcontract['intervals'].copy()
            doc_staff_intervals = doc_userid['working_details']['intervals'].copy()

            #compare
            if doc_subcontract_intervals != doc_staff_intervals:
                logging.info('rssc_sync_intervals %s\ndoc_subcontract_intervals:%r\ndoc_staff_intervals:%r' % (doc_userid['_id'], doc_subcontract_intervals, doc_staff_intervals))
                doc_updated = False
                while doc_updated == False:
                    try:
                        doc = db.get(doc_userid['_id'])
                        doc['working_details']['intervals'] = doc_subcontract_intervals
                        db.save(doc)
                        doc_updated = True
                        logging.info('rssc_sync_intervals %s synced' % doc_userid['_id'])
                    except couchdb.http.ResourceConflict:
                        send_task('notify_devs.send', ['rssc_server %s out of sync' % doc_userid['_id'], 'resource conflict, retrying update'])
        else:
            send_task('notify_devs.send', ['rssc_server %s out of sync' % doc_userid['_id'], 'working_details.intervals not found!'])
    else:
        send_task('notify_devs.send', ['rssc_server %s out of sync' % doc_userid['_id'], 'working_details not found!'])

        
@task(ignore_result=True)
def sync_working_details(userid, note=''):
    """syncs working_details.intervals
    """
    logging.info('fix %s %s' % (userid, note))
    doc_userid = db.get(userid)
    compare_working_details(doc_userid)


@task(ignore_result=True)
def check_all_working():
    data = db.view('dashboard/working_staff', include_docs=True)
    for d in data:
        doc_userid = d.doc
        compare_working_details(doc_userid)
        

@task(ignore_result=True, rate_limit='3/s')
def staff_connected(doc):
    """ignored result
    """
    insert_staff_connected_doc.apply_async((doc,))
    #connection_is_wireless.apply_async((doc,))


@task(ignore_result=True, default_retry_delay=10)
def connection_is_wireless(doc):
    """returns a string if document is found wireless
    """
    is_wireless = False

    WIRELESS_KEYS = {
        'WIMAX'     : 'Globe Wimax connection',
        'GSM 3G'    : 'GSM 3G / Sun Cellular connection',
        'SMARTBRO'  : 'Smartbro connection',
        '124.6.128.0 - 124.6.191.255' : 'Globe Wireless inetnum: 124.6.128.0 - 124.6.191.255',
    }

    nic_client = NICClient()
    flags = NICClient.WHOIS_RECURSE
    try:
        whois_string = nic_client.whois_lookup({}, doc['host'], flags)
    except Exception, exc:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.info('Failed to check if connection is wireless %r' % doc)
        logging.info(exc_type)
        logging.info(exc_value)
        logging.info('%r' % (traceback.extract_stack()))
        raise connection_is_wireless.retry(exc=exc, countdown=10)

    for k in WIRELESS_KEYS.iterkeys():
        if string.find(whois_string, k) != -1:
            is_wireless = WIRELESS_KEYS[k]

    if is_wireless != False:

        cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
            settings.PIKA_CRED['PASSWORD'])

        params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
            virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange='wireless_violators', exchange_type='fanout')

        if doc.has_key('doc_celery'):
            fname = doc['doc_celery']['fname']
            lname = doc['doc_celery']['lname']
            skype_id = doc['doc_celery']['skype_id']
            email = doc['doc_celery']['_id']
        else:
            sql = text("""
                SELECT fname, lname, skype_id, email
                FROM personal
                WHERE userid = :userid
                """)
            conn = engine.connect()
            personal = conn.execute(sql, userid=doc['userid']).fetchone()
            conn.close()

            fname = personal.fname
            lname = personal.lname
            skype_id = personal.skype_id
            email = personal.email

        message = dict(
            userid = doc['userid'],
            violation = is_wireless,
            fname = fname,
            lname = lname,
            skype_id = skype_id,
            email = email,
            host = doc['host'],
            port = doc['port'],
            )

        json_message = json.dumps(message)

        channel.basic_publish(exchange='wireless_violators', routing_key='', body=json_message)
        logging.info('wireless_violators %s' % json_message)
        connection.close()


@task(ignore_result=True, rate_limit='3/s')
def insert_staff_connected_doc(doc):
    db_rssc_connection_logs = s['rssc_connection_logs']
    if doc.has_key('doc_celery'):
        doc.pop('doc_celery')
    try:
        db_rssc_connection_logs.save(doc)
    except Exception, exc:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.info('Failed to insert connection record %r' % doc)
        logging.info(exc_type)
        logging.info(exc_value)
        logging.info('%r' % (traceback.extract_stack()))
        raise insert_staff_connected_doc.retry(exc=exc, countdown=5)


@task(ignore_result=True, rate_limit='3/s')
def staff_disconnected(doc):
    db_rssc_connection_logs = s['rssc_connection_logs']
    doc_connection = db_rssc_connection_logs.get(doc['_id'])
    if doc_connection == None:
        save_retry_disconnect = doc.get('save_retry_disconnect')
        if save_retry_disconnect == None:
            save_retry_disconnect = 0
        save_retry_disconnect += 1
        if save_retry_disconnect <= SAVE_RETRY:
            doc['save_retry_disconnect'] = save_retry_disconnect
            staff_disconnected.apply_async(args=[doc,], countdown=5)
        else:
            raise Exception('Too many retries for process staff_disconnected %s' % doc)
        return

    doc_connection['disconnected'] = doc['disconnected']
    try:
        db_rssc_connection_logs.save(doc_connection)
    except Exception, exc:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.info('Failed to update disconnection record %r' % doc)
        logging.info(exc_type)
        logging.info(exc_value)
        logging.info('%r' % (traceback.extract_stack()))
        raise staff_disconnected.retry(exc=exc, countdown=5)


@task(ignore_result=True, rate_limit='3/s')
def staff_save_platform(doc):
    db_rssc_connection_logs = s['rssc_connection_logs']
    doc_connection = db_rssc_connection_logs.get(doc['_id'])
    if doc_connection == None:
        retry_staff_save_platform = doc.get('retry_staff_save_platform')
        if retry_staff_save_platform == None:
            retry_staff_save_platform = 1
        else:
            retry_staff_save_platform += 1

        if retry_staff_save_platform >= 3:
            raise Exception('Too many retries for process staff_save_platform', '%s' % pformat(doc, 4))

        doc['retry_staff_save_platform'] = retry_staff_save_platform
        staff_save_platform.apply_async(args=[doc,], countdown=5)
        return

    doc_connection['platform'] = doc['platform']
    try:
        db_rssc_connection_logs.save(doc_connection)
    except Exception, exc:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.info('Failed to update platform connection record %r' % doc)
        logging.info(exc_type)
        logging.info(exc_value)
        logging.info('%r' % (traceback.extract_stack()))
        raise staff_save_platform.retry(exc=exc, countdown=5)


def get_skype_ids_for_dashboard_alerts():
    """
    >>> get_skype_ids_for_dashboard_alerts()
    [u'remotestaff.wilmie.u', u'remotestaff.kimberly']
    """
    conn = engine.connect()
    sql = text("""
        SELECT skype_id
        FROM admin
        WHERE rssc_dashboard_alerts = 'Y'
        AND status NOT IN ('PENDING', 'REMOVED')
        AND skype_id IS NOT NULL
    """)
    data = conn.execute(sql)
    conn.close()
    skype_ids = []

    for skype_id in data:
        skype_ids.append(skype_id[0])
    
    return skype_ids


@task(ignore_result=True, time_limit=20, rate_limit='3/s')
def disconnected_10_mins(doc_userid, time_diff):
    """publishes to mq with admin skype_ids and the name of the staff and the notice
    """
    skype_ids = get_skype_ids_for_dashboard_alerts()
    cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
        settings.PIKA_CRED['PASSWORD'])

    params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
        virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange='rssc_dashboard_alert', exchange_type='fanout')
    message = 'RSSC Disconnected for %s > %s %s userid:%s skype_id:%s email:%s ' % (time_diff, doc_userid['fname'], doc_userid['lname'], doc_userid['reference_id'], doc_userid['skype_id'], doc_userid['_id'])

    message = dict(
        skype_ids = skype_ids,
        message = message,
        )

    json_message = json.dumps(message)

    channel.basic_publish(exchange='rssc_dashboard_alert', routing_key='', body=json_message)
    logging.info('rssc_dashboard_alert %s' % json_message)
    connection.close()


def get_ph_time():
    """returns Asia/Manila time
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    return now


@task(ignore_result=True, time_limit=20, rate_limit='3/s')
def over_break(userid, time_diff):
    """publishes to mq with admin skype_ids and the name of the staff and the notice
    """
    skype_ids = get_skype_ids_for_dashboard_alerts()
    doc_userid = db.get(userid)
    if doc_userid == None:
        raise Exception('rssc_server.over_break :%s %s, userid not found on couchdb rssc' % (userid, time_diff))

    #used redis to prevent multiple sending
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    if r.get('rssc_over_quick_break:%s' % userid) != None:  #key found ignore
        return

    r.set('rssc_over_quick_break:%s' % userid, '%s' % get_ph_time())
    r.expire('rssc_over_quick_break:%s' % userid, 300)  #expire in 300 seconds/5 minutes

    cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
        settings.PIKA_CRED['PASSWORD'])

    params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
        virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange='rssc_dashboard_alert', exchange_type='fanout')
    message = 'RSSC Over Break for %s > %s %s userid:%s skype_id:%s email:%s ' % (time_diff, doc_userid['fname'], doc_userid['lname'], doc_userid['reference_id'], doc_userid['skype_id'], doc_userid['_id'])

    message = dict(
        skype_ids = skype_ids,
        message = message,
        )

    json_message = json.dumps(message)

    channel.basic_publish(exchange='rssc_dashboard_alert', routing_key='', body=json_message)
    logging.info('rssc_dashboard_alert %s' % json_message)
    connection.close()


@task(ignore_result=True, time_limit=20, rate_limit='3/s')
def over_lunch(userid, time_diff):
    """publishes to mq with admin skype_ids and the name of the staff and the notice
    """
    skype_ids = get_skype_ids_for_dashboard_alerts()
    doc_userid = db.get(userid)
    if doc_userid == None:
        raise Exception('rssc_server.over_break :%s %s, userid not found on couchdb rssc' % (userid, time_diff))

    #used redis to prevent multiple sending
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    if r.get('rssc_over_lunch_break:%s' % userid) != None:  #key found ignore
        return

    r.set('rssc_over_lunch_break:%s' % userid, '%s' % get_ph_time())
    r.expire('rssc_over_lunch_break:%s' % userid, 300)  #expire in 300 seconds/5 minutes

    cred = pika.credentials.PlainCredentials(settings.PIKA_CRED['USER'],
        settings.PIKA_CRED['PASSWORD'])

    params = pika.ConnectionParameters(settings.PIKA_CRED['HOST'], 
        virtual_host=settings.PIKA_CRED['VHOST'], credentials=cred)

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange='rssc_dashboard_alert', exchange_type='fanout')
    message = 'RSSC Over Lunch for %s > %s %s userid:%s skype_id:%s email:%s ' % (time_diff, doc_userid['fname'], doc_userid['lname'], doc_userid['reference_id'], doc_userid['skype_id'], doc_userid['_id'])

    message = dict(
        skype_ids = skype_ids,
        message = message,
        )

    json_message = json.dumps(message)

    channel.basic_publish(exchange='rssc_dashboard_alert', routing_key='', body=json_message)
    logging.info('rssc_dashboard_alert %s' % json_message)
    connection.close()


@task(ignore_result=True, time_limit=20, rate_limit='3/s')
def check_subcon_records():
    """will be called using celery beat every 5mins
    """
    # collect couchdb records
    db = s['rssc']
    subcontractor_ids_from_couchdb = []
    for r in db.view('subcontractor/active'):
        sid = r.get('id')
        sid = re.sub('subcon-', '', sid)
        sid = int(sid)
        subcontractor_ids_from_couchdb.append(sid)

    # collect mysql records
    conn = engine.connect()
    sql = text("""
        SELECT id 
        FROM subcontractors
        WHERE status = 'ACTIVE'
    """)

    sql_update_status = text("""
        UPDATE subcontractors
        SET status = 'ACTIVE'
        WHERE id = :sid
    """)

    data = conn.execute(sql)
    for d in data:
        sid = int(d.id)
        if sid not in subcontractor_ids_from_couchdb:
            logging.info('%s not active in couchdb' % sid)
            #temporarily disabled notify devs
            #send_task('notify_devs.send', ['rssc subcon-%s out of sync' % sid, 'syncing it now.'])
            doc = db.get('subcon-%s' % sid)
            if doc == None: #no records on rssc yet trigger it by setting the status once again
                logging.info('%s not on couchdb yet, update status to sync' % sid)
                conn.execute(sql_update_status, sid=sid)
            else:
                logging.info('%s already on couchdb, updating active field to Y and adding history' % sid)
                doc['active'] = 'Y'
                history = doc.get('history')
                if history == None:
                    history = []

                now = get_ph_time()
                history.append(
                    dict(
                        note = 'updates by celery rssc_server.check_subcon_records, set status from N to Y',
                        date = now.strftime('%Y-%m-%d %H:%M:%S')
                    )
                )

                doc['history'] = history
                db.save(doc)

    conn.close()



def run_tests():
    """
##~    >>> db = s['rssc_connection_logs']
##~    >>> doc_celery = dict(fname='Fname', lname='Lname', _id='devs@remotestaff.com.au', skype_id='devs-test')
##~    >>> doc = db.get('37555-1356478465.591491') #WIMAX sample
##~    >>> connection_is_wireless(doc)
##~    >>> doc = db.get('74-1356510143.855761') #GSM 3G sample
##~    >>> connection_is_wireless(doc)
##~    >>> doc = db.get('74-1356477282.388213') #SMARTBRO sample
##~    >>> connection_is_wireless(doc)
##~    >>> doc = db.get('5490-1356476709.921841') #inetnum:        124.6.128.0 - 124.6.191.255 sample
##~    >>> connection_is_wireless(doc)

##~    >>> db_rssc = s['rssc']
##~    >>> doc_userid = db_rssc.get('lawrence.sunglao@remotestaff.com.ph')
##~    >>> time_diff = timedelta(minutes=10)
##~    >>> disconnected_10_mins(doc_userid, time_diff)
##~    >>> doc_userid = db_rssc.get('normaneil.macutay@gmail.com')
##~    >>> disconnected_10_mins(doc_userid, time_diff)

##~    >>> time_diff = timedelta(minutes=10)
##~    >>> over_break('lawrence.sunglao@remotestaff.com.ph', time_diff)
##~    >>> over_break('fail@testing.com', time_diff)
    Traceback (most recent call last):
    ...
    Exception: rssc_server.over_break :fail@testing.com 0:10:00, userid not found on couchdb rssc

##~    >>> time_diff = timedelta(minutes=10)
##~    >>> over_lunch('lawrence.sunglao@remotestaff.com.ph', time_diff)

    >>> check_subcon_records()

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
