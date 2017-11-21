#   2013-11-04 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   removed debug settings
#   2013-11-02 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   set email from clientactivitynotes@remotestaff.com.au if activity note is for Chris
#   2013-08-29 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fix for some activity notes settings with no activity notes and has time records only
#   2013-08-24 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   replace the php cron job portal/cronjobs/activity_tracker_notes.php
#   -   Client Activity Notes Return Email Task #4382
#   2013-05-30 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task for retrieving activity notes

import settings
import couchdb
from celery.task import task
from celery.execute import send_task
from datetime import datetime, timedelta, time
from pytz import timezone

from persistent_mysql_connection import engine
from sqlalchemy.sql import text
from pprint import pprint, pformat
import string

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import re
from Cheetah.Template import Template

from pymongo import MongoClient
from bson.objectid import ObjectId


def safe_mongocall(call):
    def _safe_mongocall(*args, **kwargs):
        for i in xrange(5):
            try:
                return call(*args, **kwargs)
            except pymongo.AutoReconnect:
                time.sleep(pow(2, i))
        print 'Error: Failed operation!'
    return _safe_mongocall


@safe_mongocall
def insert_to_collection(col, record):
    col.insert(record)

@safe_mongocall
def find_one(col, params):
    return col.find_one(params)

@safe_mongocall
def find_all(col, params):
    return col.find(params)


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
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


@task
def get_notes(userid, start_date_str, end_date_str, timezone_str, skip, limit, client_id):
    """get notes
    start_date_str and end_date_str expected in timezone_str timezon
    """
    conn = engine.connect()
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_activity_notes']

    #get staff name
    sql = text("""
        SELECT fname, lname
        FROM personal
        WHERE userid = :userid
    """)
    staff = conn.execute(sql, userid=userid).fetchone()
    fname = string.capwords(string.strip(staff.fname))
    lname = string.capwords(string.strip(staff.lname))

    notes_tz = timezone(timezone_str)
    ph_tz = timezone('Asia/Manila')

    x = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
    y = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')

    #set dates according to timezone_str tz parameter
    x = datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, tzinfo=notes_tz)
    y = datetime(y.year, y.month, y.day, y.hour, y.minute, y.second, tzinfo=notes_tz)

    #set dates to Asia/Manila tz
    x = x.astimezone(ph_tz)
    y = y.astimezone(ph_tz)

    date_to_execute_from = x
    date_to_execute_to = y

    x = x.timetuple()
    y = y.timetuple()

    clients = {}    #list of client for faster or in memory lookups

    if client_id == 'all':
        data = db.view('activity_note/userid_date', 
            startkey=[userid, [x[0], x[1], x[2], x[3], x[4], x[5]]], 
            endkey=[userid, [y[0], y[1], y[2], y[3], y[4], y[5]]],
            include_docs=True, skip=skip, limit=limit)
    else:
        data = db.view('activity_note/userid_leadsid_date', 
            startkey=[userid, client_id, [x[0], x[1], x[2], x[3], x[4], x[5]]], 
            endkey=[userid, client_id, [y[0], y[1], y[2], y[3], y[4], y[5]]],
            include_docs=True, skip=skip, limit=limit)

    activity_notes = []
    for d in data:
        doc = d.doc

        #check if client is on list, retrieve if not
        if clients.has_key(doc['leads_id']) == False:
            sql = text("""SELECT fname, lname, email, status FROM leads WHERE id=:client_id""")
            r = conn.execute(sql, client_id=doc['leads_id']).fetchone()
            clients[doc['leads_id']] = dict(fname=r.fname, lname=r.lname, email=r.email)

        req = doc['requested']
        res = doc['responded']
        req = datetime(req[0], req[1], req[2], req[3], req[4], req[5])
        res = datetime(res[0], res[1], res[2], res[3], res[4], res[5])
        req = ph_tz.localize(req)
        res = ph_tz.localize(res)

        #set to requested tz
        req = req.astimezone(notes_tz)
        res = res.astimezone(notes_tz)

        activity_notes.append(
            dict(
                note = doc['note'],
                requested = req,
                responded = res,
                status = doc['status'],
                client_id = doc['leads_id'],
            )
        )

    #get timerecords, used in conjunction with activity tracker notes email
    db_time_records = s['rssc_time_records']
    data = db_time_records.view('rssc_reports/userid_timein',
        startkey=[userid, [x[0], x[1], x[2], x[3], x[4], x[5]]], 
        endkey=[userid, [y[0], y[1], y[2], y[3], y[4], y[5]]],
        )

    time_records = []
    lunch_records = []
    
    time_records_count = 0
    for d in data:
        time_records_count += 1
        doc_type, doc_time_out, doc_client_id, sid = d['value']
        if client_id != 'all':
            if client_id != doc_client_id:
                continue

        if doc_type == 'quick break':
            continue

        time_in = d['key'][1]
        time_type, time_out, client_id, subcontractors_id = d['value']

        #convert time
        x = datetime(time_in[0], time_in[1], time_in[2], time_in[3], time_in[4], time_in[5])
        x = ph_tz.localize(x)
        time_in = x.astimezone(notes_tz)

        if time_out in (None, False):
            time_out = None
        else:
            y = datetime(time_out[0], time_out[1], time_out[2], time_out[3], time_out[4], time_out[5])
            y = ph_tz.localize(y)
            time_out = y.astimezone(notes_tz)

        if doc_type == 'time record':
            time_records.append(dict(
                time_in = time_in, 
                time_out = time_out, 
                client_id = client_id, 
                subcontractors_id = subcontractors_id,
            ))
        elif doc_type == 'lunch record':
            lunch_records.append(dict(
                time_in = time_in, 
                time_out = time_out, 
                client_id = client_id, 
                subcontractors_id = subcontractors_id,
            ))

    time_records_merged = []
    #loop over time_records for matching with lunch records
    for record in time_records:
        time_in = record.get('time_in')
        time_out = record.get('time_out')
        lunch_match = None
        for lunch_record in lunch_records:
            lunch_record_in = lunch_record.get('time_in')
            if time_out == None:    #record not closed yet, just get whats greater than
                if lunch_record_in > time_in:
                    lunch_match = lunch_record
            else:
                if lunch_record_in >= time_in and lunch_record_in <= time_out:
                    lunch_match = lunch_record

        if lunch_match != None:
            record['lunch_start'] = lunch_match.get('time_in')
            record['lunch_finish'] = lunch_match.get('time_out')

        time_records_merged.append(record)

    #get subcon_id by cross checking client_id and userid from subcontractors table
    subcon_ids = []
    if client_id != 'all':
        sql = text("""
            SELECT id
            FROM subcontractors
            WHERE leads_id = :client_id
            AND userid = :userid
            """)
        data = conn.execute(sql, client_id=client_id, userid=userid).fetchone()
        if data != None:
            subcon_ids.append(data.id)

    #grab notes from tb_admin_activity_tracker_notes
    admin_notes_per_staff = []
    if len(subcon_ids) > 0:
        for subcon_id in subcon_ids:
            sql = text("""SELECT note FROM tb_admin_activity_tracker_notes
                WHERE client_id = :client_id
                and subcon_id = :subcon_id
                and method = "PER STAFF"
                and status = "ACTIVE"
                and date_to_execute_from <= :date_to_execute_from
                and date_to_execute_to >= :date_to_execute_to
                """)
            admin_note_per_staff = conn.execute(sql, client_id=client_id, subcon_id=subcon_id, date_to_execute_from=date_to_execute_from, date_to_execute_to=date_to_execute_to).fetchall()
            if len(admin_note_per_staff) != 0:
                admin_notes_per_staff.append(admin_note_per_staff)

    conn.close()
    return dict(
        activity_notes = activity_notes,
        clients = clients,
        time_records = time_records_merged,
        fname = fname,
        lname = lname,
        admin_notes_per_staff = admin_notes_per_staff,
        time_records_count = time_records_count
    )


@task
def email_notes(subject, email_to, email_cc, email_bcc, userids, start_date_str, end_date_str, timezone_str, client_id):
    """
    start_date_str and end_date_str expected in timezone_str timezon
    """

    now = get_ph_time()
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second] 
    is_sent = False
    
    #Check if activity tracker notes has already been sent to client
    phtz = timezone('Asia/Manila')
    end = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=phtz)
    start = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=phtz)

    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)            
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017) 
                    
    db = mongo_client.prod
    col = db.daily_activity_tracker_notes

    if client_id != 'all':
        mongo_doc=find_one(col, {'client_id' : int(client_id), 'date_created' : {'$lt': end, '$gte': start} })

        if mongo_doc != None:
            is_sent = True
            logging.info("Activity notes has already been sent to client Id %s." % client_id)
            return "Activity notes has already been sent to your email."

        if mongo_doc == None:                
            record={            
                'client_id' : int(client_id),
                'date_created' : now        
            }
            insert_to_collection(col, record)
            is_sent = False

    if is_sent == False:        
        #Start 
        conn = engine.connect()
        notes = []
        activity_notes_count = 0
        time_records_count = 0

        #sort userids by first name/last name
        userids_string = '%s' % userids
        userids_string = re.sub('\[', '(', userids_string)
        userids_string = re.sub('\]', ')', userids_string)
        x = """
            SELECT userid
            FROM personal
            WHERE userid IN %s
            ORDER BY fname, lname
        """ % userids_string
        sql = text(x)
        sorted_userids = conn.execute(sql).fetchall()

        for userid in sorted_userids:
            userid = int(userid[0])
            note = get_notes(userid, start_date_str, end_date_str, timezone_str, 0, 500, client_id)
            notes.append(note)
            activity_notes_count += len(note['activity_notes'])
            time_records_count += len(note['time_records'])

        if activity_notes_count == 0 and time_records_count == 0:
            return "No notes to send.\nPlease check your dates."

        #get admin notes that are method='ALL'
        sql = text("""SELECT note 
            FROM tb_admin_activity_tracker_notes
            WHERE method = "ALL"
            AND status = "ACTIVE"
            AND date_to_execute_from <= :start_date_str
            AND date_to_execute_to >= :end_date_str
            """)
        admin_notes_all = conn.execute(sql, start_date_str=start_date_str, end_date_str=end_date_str).fetchall()

        #get admin notes that are method='PER CLIENT'
        admin_notes_per_client = []
        if client_id != 'all':
            sql = text("""select note from tb_admin_activity_tracker_notes
                where client_id = :client_id
                and method = "PER CLIENT"
                and status = "ACTIVE"
                and date_to_execute_from <= :start_date_str
                and date_to_execute_to >= :end_date_str
                """)
            admin_notes_per_client = conn.execute(sql, client_id=client_id, start_date_str=start_date_str, end_date_str=end_date_str).fetchall()

        #get admin notes that are method='PER COUNTRY CITY'
        #get first the country_city
        if client_id != 'all':
            sql = text("""
                SELECT tl.timezone
                FROM leads AS l
                JOIN timezone_lookup AS tl
                ON l.timezone_id = tl.id
                WHERE l.id = :client_id
                """)
            data = conn.execute(sql, client_id=client_id).fetchone()
            if data == None or data == False:
                client_timezone = 'Australia/Sydney'
            elif len(data) == 0:
                client_timezone = 'Australia/Sydney'
            else:
                client_timezone = conn.execute(sql, client_id=client_id).fetchone()[0]

        admin_notes_per_country_city = []
        if client_id != 'all':
            sql = text("""SELECT note
                FROM tb_admin_activity_tracker_notes
                WHERE client_id = :client_id
                AND method = "PER COUNTRY CITY"
                AND status = "ACTIVE"
                AND country_city = :client_timezone
                AND date_to_execute_from <= :start_date_str
                AND date_to_execute_to >= :end_date_str
                """)
            admin_notes_per_country_city = conn.execute(sql, client_id=client_id, client_timezone=client_timezone, start_date_str=start_date_str, end_date_str=end_date_str).fetchall()

        t = Template(file="templates/activity_tracker_notes.tmpl")

        #assign variables to the template file
        t.admin_notes_all = admin_notes_all
        t.admin_notes_per_client = admin_notes_per_client
        t.admin_notes_per_country_city = admin_notes_per_country_city 
        t.notes = notes
        t.client_timezone = timezone_str

        s = couchdb.Server(settings.COUCH_DSN)
        couch_mailbox = s['mailbox']

         		
        mailbox_doc = dict(
            sent = False,		
            bcc = email_bcc,
            cc = email_cc,
            created = date_created,
            generated_by = 'celery activity_tracker_notes.email_notes',
            html = '%s' % t,
            subject = subject,
            to = email_to,
        )
        if client_id == 'all':
            email_from = 'csro@remotestaff.com.au'
        else:
            if int(client_id) == 11:    #special request by rica on activity notes sent to chris
                email_from = 'clientactivitynotes@remotestaff.com.au'
            else:
                t = send_task("EmailSender.GetCsroEmail", [client_id])
                email_from = t.get()
                if email_from == None:
                    email_from = 'csro@remotestaff.com.au'

        mailbox_doc['from'] = email_from
        couch_mailbox.save(mailbox_doc)

        conn.close()

        return "Activity notes has been sent to your email."


def filter_remove_blanks(x):
    if string.strip(x) == '':
        return False
    else:
        return True


@task(ignore_result=True)
def daily_cron(execute_time='now'):
    """
    replaces the php cron job portal/cronjobs/activity_tracker_notes.php
    must be executed at exactly 10s or might have overlaps
    expects execute_time in '2013-08-25 18:14:00' format
    """

    if execute_time == 'now':
        now = get_ph_time()
    else:
        now = datetime.strptime(execute_time, '%Y-%m-%d %H:%M:%S')

    date_start = now.replace(second=0, microsecond=0)    #zero out
    date_end = date_start + timedelta(minutes=9)
    date_past_day = date_start - timedelta(days=1)

    #used for couchdb date queries
    a = [date_past_day.year, date_past_day.month, date_past_day.day, date_past_day.hour, date_past_day.minute, date_past_day.second]
    b = [date_start.year, date_start.month, date_start.day, date_start.hour, date_start.minute, date_start.second]

    # get only the time
    time_start = time(date_start.hour, date_start.minute, date_start.second)
    time_end = time(date_end.hour, date_end.minute, date_end.second)

    conn = engine.connect()
    sql = text("""
        SELECT id, client_id, type, hour, minute,
        client_timezone, send_time, status,
        email, cc
        FROM tb_client_account_settings
        WHERE status = "ALL"
        AND send_time BETWEEN :start and :end GROUP BY client_id 
    """)
    client_settings = conn.execute(sql, start=time_start, end=time_end)
    for client_setting in client_settings:
        # get active/suspended subcontractors record
        sql = text("""
            SELECT id, userid
            FROM subcontractors
            WHERE status IN ('ACTIVE', 'suspended')
            AND leads_id = :leads_id
        """)
        subcontractors = conn.execute(sql, leads_id=client_setting.client_id)
        sids_userids = []
        for x in subcontractors:
            sids_userids.append([x.id, x.userid])

        if len(sids_userids) == 0:
            continue

        # get timerecords via couchdb, 
        # if no timerecords found, dont send activity note
        timerecords = 0

        s = couchdb.Server(settings.COUCH_DSN)
        db = s['rssc_time_records']

        userids = []
        for sid, userid in sids_userids:
            sid = int(sid)
            userid = int(userid)

            if userid not in userids:
                userids.append(userid)

            result = db.view('prepaid/timerecords', 
                startkey=[userid, sid, a],
                endkey=[userid, sid, b])

            for r in result:
                timerecords += 1

        if timerecords == 0:    # no timerecords found, dont send
            continue

        if client_setting.email in (None, ''):  #use leads.email
            sql = text("""
                SELECT email
                FROM leads
                WHERE id = :leads_id
            """)
            to = [conn.execute(sql, leads_id=client_setting.client_id).fetchone()[0]]
        else:
            to = re.split(',|;| ', client_setting.email)
            to = filter(filter_remove_blanks, to)

        if client_setting.cc in (None, ''):
            cc = []
        else:
            cc = re.split(',|;| ', client_setting.cc)
            cc = filter(filter_remove_blanks, cc)

        client_timezone = client_setting.client_timezone
        if client_timezone == None:
            client_timezone = 'Australia/Sydney'
        elif string.strip(client_timezone) == '':
            client_timezone = 'Australia/Sydney'

        phtz = timezone('Asia/Manila')
        client_tz = timezone(client_timezone)

        ph_date = datetime(date_start.year, date_start.month, date_start.day, date_start.hour, date_start.minute, date_start.second, tzinfo=phtz)
        client_date = ph_date.astimezone(client_tz)

        x = datetime(date_past_day.year, date_past_day.month, date_past_day.day, date_past_day.hour, date_past_day.minute, date_past_day.second, tzinfo=phtz)
        y = datetime(date_start.year, date_start.month, date_start.day, date_start.hour, date_start.minute, date_start.second, tzinfo=phtz)
        date_past_day_client = x.astimezone(client_tz)
        date_start_client = y.astimezone(client_tz)
            
        subject = 'Remote Staff Daily Activity Tracker Notes %s' % client_date.strftime('%b %d')
        recipients=[]		
        recipients.append('lawrence.sunglao@remotestaff.com.au') 
        recipients.append('charisse.m@remotestaff.com.au')
			
        result = email_notes(subject, to, cc, recipients, userids, date_past_day_client.strftime('%F %H:%M:%S'), date_start_client.strftime('%F %H:%M:%S'), client_timezone, client_setting.client_id)

    conn.close()


def run_tests():
    """
##~    >>> notes = get_notes(69, '2012-12-17 00:00:00', '2013-12-17 23:59:59', 'America/Chicago', 0, 100, 11)
##~    >>> notes = get_notes(20622, '2012-11-14 00:00:00', '2012-11-15 23:59:59', 'Asia/Manila', 0, 30, 6600)
##~    >>> notes = get_notes(20622, '2012-11-14 00:00:00', '2012-11-15 23:59:59', 'Australia/Sydney', 0, 3, 6311)
##~    >>> notes = get_notes(20622, '2012-11-14 00:00:00', '2012-11-15 23:59:59', 'Australia/Sydney', 0, 100, 'all')
##~    >>> notes = get_notes(51034, '2013-05-14 00:00:00', '2013-05-30 23:59:59', 'Australia/Sydney', 0, 100, 'all')
##~    >>> email_notes('Remote Staff Daily Activity Tracker Notes', ['test@12345.com','devs@testing123'], [], [], [69], '2008-01-01 00:00:00', '2008-02-01 00:00:00', 'Asia/Manila', 'all')
##~    'No notes to send.\\nPlease check your dates.'

##~    >>> email_notes('Remote Staff Daily Activity Tracker Notes', ['test@12345.com','devs@testing123'], [], [], [51034], '2013-05-28 00:00:00', '2013-05-29 23:59:59', 'Asia/Manila', 'all')
##~    'Activity notes has been sent to your email.'
##~    >>> email_notes('Remote Staff Daily Activity Tracker Notes', ['test@12345.com','devs@testing123'], [], [], [55757, 73153, 8618, 76190, 3382, 69, 74, 49711, 47657], '2013-05-28 00:00:00', '2013-05-29 23:59:59', 'Asia/Manila', 'all')
##~    'Activity notes has been sent to your email.'

##~    >>> daily_cron()
##~    >>> daily_cron('2013-08-22 07:00:00')

##~    >>> daily_cron('2013-08-22 17:00:00')
##~    >>> daily_cron('2013-08-22 18:00:00')
##~    >>> daily_cron('2013-08-22 20:00:00')
    >>> daily_cron('2013-05-14 20:00:00')

##~    >>> daily_cron('2013-08-25 18:00:00')

##~    >>> daily_cron('2013-08-25 20:00:00')

    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
