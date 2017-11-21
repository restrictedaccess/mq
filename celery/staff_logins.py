#   2014-05-02  Normaneil Macutay<normanm@remotestaff.com.au>
#   - Initial Hack.  Task to get subcon login date/time in couchdb rssc_time_records

from sqlalchemy.sql import text
from persistent_mysql_connection import engine

import settings
import couchdb

from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet

from datetime import date, datetime, timedelta
from pytz import timezone
from decimal import Decimal

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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
def get_last_login_date(userid, sid, from_date, to_date):
    now = get_ph_time()
    from_date  = datetime.strptime(from_date, '%Y-%m-%d')
    to_date  = datetime.strptime(to_date, '%Y-%m-%d')
    descending=True	
    if from_date < now:	
        descending=False
    from_year = '%s' % from_date.strftime('%Y')
    from_month = '%s' % from_date.strftime('%m')
    from_day = '%s' % from_date.strftime('%d')
	
    to_year = '%s' % to_date.strftime('%Y')
    to_month = '%s' % to_date.strftime('%m')
    to_day = '%s' % to_date.strftime('%d')	
	
    login=""
    last_login_date=""
    last_login_time_in=""	
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']	
    userid = int(userid)	
    if descending:
        r = db.view('rssc_reports/userid_timein', 
            startkey=[userid, [ int(from_year), int(from_month), int(from_day),0,0,0,0]], 
            endkey=[userid, [ int(to_year), int(to_month), int(to_day),23,59,59,0]],
            descending=True		
        )	
    else:	
        r = db.view('rssc_reports/userid_timein', 
            startkey=[userid, [ int(from_year), int(from_month), int(from_day),0,0,0,0]], 
            endkey=[userid, [ int(to_year), int(to_month), int(to_day),23,59,59,0]],
            ascending=True		
        )
    
    login_dates=[]	
    str=""	
    for row in r.rows:
        	
        mode, time_out, leads_id, subcon_id = row['value']
        userid, time_in = row['key']
        #str +='%s<br>' % mode   		
        	
        if mode == 'time record':
            if int(sid) == int(subcon_id):		
                login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), 0, 0, 0)
                if '%s' % login.strftime('%Y-%m-%d') not in login_dates:
                    login_dates.append(login)				
                
            			
    #staff last login date
    if login_dates:	
        last_login_date =  max(login_dates)
    
    if last_login_date: 	
        r = db.view('rssc_reports/userid_timein', 
            startkey=[userid, [ int(last_login_date.strftime('%Y')), int(last_login_date.strftime('%m')), int(last_login_date.strftime('%d')),0,0,0,0]],
            endkey=[userid, [ int(last_login_date.strftime('%Y')), int(last_login_date.strftime('%m')), int(last_login_date.strftime('%d')),23,59,59,0]], 
            ascending=True		
        )
        for row in r.rows:       	
            mode, time_out, leads_id, subcon_id = row['value']
            userid, time_in = row['key']
                    	
            if mode == 'time record':
                if int(sid) == int(subcon_id):
                    last_login_time_in = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5])) 
					
        return last_login_time_in



@task
def get_rssc_time_records(userid, sid, from_date, to_date):
    from_date  = datetime.strptime(from_date, '%Y-%m-%d')
    to_date  = datetime.strptime(to_date, '%Y-%m-%d')
    #end_date_object = to_date_object + timedelta(days=1)
	
    from_year = '%s' % from_date.strftime('%Y')
    from_month = '%s' % from_date.strftime('%m')
    from_day = '%s' % from_date.strftime('%d')
	
    to_year = '%s' % to_date.strftime('%Y')
    to_month = '%s' % to_date.strftime('%m')
    to_day = '%s' % to_date.strftime('%d')	
	
    login=""
    last_login_date=""
    last_login_time_in=""	
    
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']	
	
    userid = int(userid)				
    r = db.view('rssc_reports/userid_timein', 
        startkey=[userid, [ int(from_year), int(from_month), int(from_day),0,0,0,0]], 
        endkey=[userid, [ int(to_year), int(to_month), int(to_day),23,59,59,0]],
        ascending=True		
    )

    str=""
    rssc_records=[]	
    for row in r.rows:
        	
        mode, time_out, leads_id, subcon_id = row['value']
        userid, time_in = row['key']
        		
        c=0		
        logout=time_out
        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))	
        if time_out:
		
            logout = datetime(int(time_out[0]), int(time_out[1]), int(time_out[2]), int(time_out[3]), int(time_out[4]), int(time_out[5]))            
            c =  logout - login #timedelta object        		
            c = ((c.days * 86400) + c.seconds) / 60.0 / 60.0			
            c = '%0.2f' % c	
        			
        if not time_out:
            today = datetime.now()            
            c =  today - login #timedelta object        		
            c = ((c.days * 86400) + c.seconds) / 60.0 / 60.0			
            c = '%0.2f' % c	    
			
        date_time_in = login.strftime('%Y-%m-%d')
        hour_time_in = login.strftime('%Y-%m-%d %H:%M:%S %p')
		
        		

        if mode != 'quick break':
            if int(sid) == int(subcon_id):		
                str += '%s %s %s<br>' % (login, logout, c)
                rssc_records.append(dict(
                    date_time_in = date_time_in,			
                    hour_time_in = login, 			
                    hour_time_out = logout,
                    hours = c,
                    mode = mode				
                    )
                )			
            			
    		
    return rssc_records		

@task
def get_compliance_result(subcon_id, date_str, userid, leads_id, complete=False):
    work=""
    work = work_schedule(subcon_id, date_str)
    starting_date = work['starting_date']
    work_days = work['work_days']
    flexi = work['flexi']	
    compliance =""
    timein=""
    num_of_leave =0
    num_of_marked_absent=0
	
    conn = engine.connect()				
    sql="SELECT COUNT(l.id)AS num_of_leave FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE l.date_of_leave='%s' AND l.status='approved' AND r.userid=%s and r.leads_id=%s" % (date_str, userid, leads_id)
    num_of_leave = conn.execute(sql).fetchone()
    num_of_leave = int('%d' % num_of_leave.num_of_leave)

	
    sql="SELECT COUNT(l.id)AS num_of_marked_absent FROM leave_request_dates l JOIN leave_request r ON r.id = l.leave_request_id WHERE l.date_of_leave='%s' AND l.status='absent' AND r.userid=%s and r.leads_id=%s" % (date_str, userid, leads_id)
    num_of_marked_absent = conn.execute(sql).fetchone()
    num_of_marked_absent = int('%d' % num_of_marked_absent.num_of_marked_absent)
    
	
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']
	
    d = datetime.strptime(date_str, '%Y-%m-%d')
    from_year = '%s' % d.strftime('%Y')
    from_month = '%s' % d.strftime('%m')
    from_day = '%s' % d.strftime('%d')
    time_in=""
	
    userid = int(userid)				
    r = db.view('rssc_reports/userid_timein', 
        startkey=[userid, [ int(from_year), int(from_month), int(from_day),0,0,0,0]], 
        endkey=[userid, [ int(from_year), int(from_month), int(from_day),23,59,59,0]],
        descending=False			
    )
    for row in r.rows:            		
        mode, time_out, leads_id, subcon_id = row['value']           			
        if mode == 'time record' :
            userid, time_in = row['key']
            break
			
    day = d.strftime('%a')			
    day = day.lower()	
	
    five_minutes_before_time=""	
    five_minutes_after_time=""
    two_hours_after_time =""
    ten_minutes_before_time=""
        
    current_time = datetime.now()	
    #return work['staff_start_work_hour']	
    if time_in:			
        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))			
        if d.strftime('%Y-%m-%d') == login.strftime('%Y-%m-%d'):
            timein = login
				
            				
        five_minutes_before_time = get_five_minutes_before_time(work['staff_start_work_hour'])
        five_minutes_after_time = get_five_minutes_after_time(work['staff_start_work_hour'])			
        if day not in work_days:
            if flexi == 'no':		
                compliance = 'extra day'
            else:
                compliance = 'flexi'			
        if day in work_days:
            if flexi == 'no':  		
                if timein < five_minutes_before_time:
                    compliance = 'early login'
					
                if timein > five_minutes_after_time:
                    compliance = 'late'

                if timein >= five_minutes_before_time and timein <= five_minutes_after_time:
                    compliance = 'present'
            else:
                compliance = 'flexi' 			
				
    else:
        if int(num_of_marked_absent) > 0:
            compliance = 'absent'		
        if int(num_of_marked_absent) == 0:	
            if day in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'		
                else:
                    two_hours_after_time = get_two_hours_after_time(work['staff_start_work_hour'])
                    if flexi == 'no':
                        if complete: # used by Running Late page
                            if current_time < two_hours_after_time:					
                                if work['staff_start_work_hour'] < current_time:
                                    compliance = 'running late'
                                else:
                                    if current_time >= get_ten_minutes_before_time(work['staff_start_work_hour']) and current_time <= work['staff_start_work_hour']:
                                        compliance = '10 minutes'							
                                    else:
                                        compliance = "not yet working"
                            else:
                                if int(num_of_leave) > 0:
                                    compliance='on leave'

                                else:					
                                    compliance = 'absent'
						
                        else:					
                            if current_time < two_hours_after_time:
                                compliance = "not yet working"
                            else:
                                compliance = 'absent'
                    else:
                        compliance = 'flexi'				
				
            if day not in work_days:

                if int(num_of_leave) > 0:
                    compliance='on leave'

                else:					
                    compliance = 'no schedule'			
        			
    if datetime(starting_date.year, starting_date.month, starting_date.day) > datetime(d.year, d.month, d.day):
        compliance = "not yet working"

		
    data = dict(
        compliance = compliance,
        work_schedule = work,		
        staff_start_work_hour= work['staff_start_work_hour'],
        timein = timein		
    )
    return data
	
	
@task	
def work_schedule(subcon_id, date_str):
    
    conn = engine.connect()
    date_str  = datetime.strptime(date_str, '%Y-%m-%d')
    dayname_str = '%s' % date_str.strftime('%a')
    dayname_str = '%s' % dayname_str.lower()
		
    	
    #get subcon	
    sql = text("""SELECT id, work_status, staff_working_timezone, client_timezone, work_days, 
        mon_start, mon_finish, mon_number_hrs, 
        tue_start, tue_finish, tue_number_hrs,
        wed_start, wed_finish, wed_number_hrs,
        thu_start, thu_finish, thu_number_hrs,
        fri_start, fri_finish, fri_number_hrs,
        sat_start, sat_finish, sat_number_hrs,
        sun_start, sun_finish, sun_number_hrs,
        client_start_work_hour, client_finish_work_hour, starting_date, flexi		
        FROM subcontractors 
        WHERE id=:subcon_id""")		
    subcon = conn.execute(sql, subcon_id=subcon_id).fetchone()
	
	
    try:
        ph_tz = timezone(subcon.staff_working_timezone)#timezone('Asia/Manila')
    except TypeError:
        pass
    except:
        ph_tz = timezone('Asia/Manila') 
    else:
        ph_tz = timezone(subcon.staff_working_timezone)#timezone('Asia/Manila')
			
    client_tz = timezone(subcon.client_timezone)	

    work_days=['mon', 'tue', 'wed', 'thu', 'fri']
    staff_start_work_hour=None
    staff_finish_work_hour=""
    client_start_work_hour =""	
    client_finish_work_hour=""
    have_sched = True
    regular_contract_hrs=0

    if subcon.work_days:
        work_days = subcon.work_days.split(',')

        if dayname_str in work_days:
            have_sched = True		
            if dayname_str == 'mon':
                staff_start_work_hour = subcon.mon_start
                staff_finish_work_hour = subcon.mon_finish
                regular_contract_hrs = subcon.mon_number_hrs 				
				
            if dayname_str == 'tue':
                staff_start_work_hour = subcon.tue_start
                staff_finish_work_hour = subcon.tue_finish
                regular_contract_hrs = subcon.tue_number_hrs 				
				
            if dayname_str == 'wed':
                staff_start_work_hour = subcon.wed_start
                staff_finish_work_hour = subcon.wed_finish
                regular_contract_hrs = subcon.wed_number_hrs				
				
            if dayname_str == 'thu':
                staff_start_work_hour = subcon.thu_start
                staff_finish_work_hour = subcon.thu_finish
                regular_contract_hrs = subcon.thu_number_hrs				
				
            if dayname_str == 'fri':
                staff_start_work_hour = subcon.fri_start
                staff_finish_work_hour = subcon.fri_finish				
                regular_contract_hrs = subcon.fri_number_hrs				
				
            if dayname_str == 'sat':
                staff_start_work_hour = subcon.sat_start
                staff_finish_work_hour = subcon.sat_finish				
                regular_contract_hrs = subcon.sat_number_hrs				
				
            if dayname_str == 'sun':
                staff_start_work_hour = subcon.sun_start				
                staff_finish_work_hour = subcon.sun_finish
                regular_contract_hrs = subcon.sun_number_hrs				
            staff_start_work_hour = str(staff_start_work_hour)
            staff_finish_work_hour = str(staff_finish_work_hour)			
        if dayname_str not in work_days:
            have_sched = False
    
    if subcon.client_start_work_hour:	
        client_start_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon.client_start_work_hour))
        client_start_work_hour = client_tz.localize(datetime.strptime(client_start_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(client_tz)		
        #client_start_work_hour = client_start_work_hour.strftime('%I:%M %p')		
		
    #Staff working hours
    if not staff_start_work_hour or not have_sched:		
        staff_start_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon.client_start_work_hour))
        staff_start_work_hour = client_tz.localize(datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)       			
        staff_start_work_hour = staff_start_work_hour.strftime('%H:%M:%S')
    
        	
				
    if subcon.client_finish_work_hour: 	
        client_finish_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon.client_finish_work_hour))	
        client_finish_work_hour = client_tz.localize(datetime.strptime(client_finish_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(client_tz)
        #client_finish_work_hour = client_finish_work_hour.strftime('%I:%M %p')		

    if not staff_finish_work_hour or not have_sched: 		
        staff_finish_work_hour = '%s %s' % (datetime.now().strftime('%Y-%m-%d'), (subcon.client_finish_work_hour))
        staff_finish_work_hour = client_tz.localize(datetime.strptime(staff_finish_work_hour, '%Y-%m-%d %H:%M:%S')).astimezone(ph_tz)
        staff_finish_work_hour = staff_finish_work_hour.strftime('%H:%M:%S')		
	

    staff_start_work_hour =  '%s %s' % (date_str.strftime('%Y-%m-%d'), staff_start_work_hour)
    staff_start_work_hour = datetime.strptime(staff_start_work_hour, '%Y-%m-%d %H:%M:%S')
	
    staff_finish_work_hour =  '%s %s' % (date_str.strftime('%Y-%m-%d'), staff_finish_work_hour)
    staff_finish_work_hour = datetime.strptime(staff_finish_work_hour, '%Y-%m-%d %H:%M:%S')	

    #return staff_start_work_hour		
    return dict(
        client_start_work_hour = client_start_work_hour,
        client_finish_work_hour = client_finish_work_hour,
        client_timezone = subcon.client_timezone,
        staff_start_work_hour = staff_start_work_hour,
        staff_finish_work_hour = staff_finish_work_hour, 
        staff_working_timezone = subcon.staff_working_timezone,
        regular_contract_hrs = regular_contract_hrs,
        work_days = work_days,
        starting_date = subcon.starting_date,
        flexi =subcon.flexi 		
    )	

def get_five_minutes_before_time(time):
    time = time - timedelta(minutes=5)
    return time	
	
def get_five_minutes_after_time(time):
    time = time + timedelta(minutes=5)
    return time	

def get_ten_minutes_before_time(time):
    time = time - timedelta(minutes=10)
    return time
	
def	get_two_hours_after_time(time):
    time = time + timedelta(hours=2)
    return time


def process_query(sql):
    conn = engine.connect()
    subcons = conn.execute(sql).fetchall()	    
    data=[]
    for s in subcons:
        data.append(int(s.subcon_id))	
    return data	

@task	
def get_total_log_hrs(sid, userid, start_date, end_date):	
    total_hrs = Decimal('0.00')
    start_date  = datetime.strptime(start_date, '%Y-%m-%d')
    end_date  = datetime.strptime(end_date, '%Y-%m-%d')	
    
	
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc_time_records']	
	
    userid = int(int(userid))				
    r = db.view('rssc_reports/userid_timein', 
        startkey=[userid, [ int(start_date.strftime('%Y')), int(start_date.strftime('%m')), int(start_date.strftime('%d')),0,0,0,0]], 
        endkey=[userid, [ int(end_date.strftime('%Y')), int(end_date.strftime('%m')), int(end_date.strftime('%d')),23,59,59,0]],
        ascending=True		
    )

    for row in r.rows:
        	
        mode, time_out, leads_id, subcon_id = row['value']
        userid, time_in = row['key']
        c=0
        if subcon_id != None:		
            if int(subcon_id) == int(sid):		
                if mode == 'time record':		
                    if time_out:                
                        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))			
                        logout = datetime(int(time_out[0]), int(time_out[1]), int(time_out[2]), int(time_out[3]), int(time_out[4]), int(time_out[5]))            
                        c =  logout - login #timedelta object        		
                        c = ((c.days * 86400) + c.seconds) / 60.0 / 60.0			
                        c = Decimal('%0.2f' % c)
                        total_hrs += c
						
                if mode == 'lunch record':		
                    if time_out:                
                        login = datetime(int(time_in[0]), int(time_in[1]), int(time_in[2]), int(time_in[3]), int(time_in[4]), int(time_in[5]))			
                        logout = datetime(int(time_out[0]), int(time_out[1]), int(time_out[2]), int(time_out[3]), int(time_out[4]), int(time_out[5]))            
                        c =  logout - login #timedelta object        		
                        c = ((c.days * 86400) + c.seconds) / 60.0 / 60.0			
                        c = Decimal('%0.2f' % c)
                        total_hrs -= c						

    return "%0.2f" % total_hrs
	
@task
def get_total_adj_hrs(sid, start_date, end_date):
    total_hrs = Decimal('0.00')
    start_date  = datetime.strptime(start_date, '%Y-%m-%d')
    end_date  = datetime.strptime(end_date, '%Y-%m-%d')
	
    x = start_date
    start_date_ts = date(x.year, x.month, 1)

    #get all involved timesheet
    sql = text("""
        SELECT id, month_year FROM timesheet
        WHERE status IN ('open', 'locked')
        AND subcontractors_id = :sid
        AND month_year BETWEEN :start_date AND :end_date
    """)
    conn = engine.connect()
    timesheets = conn.execute(sql, sid=sid, start_date=start_date_ts, end_date=end_date).fetchall()

    for ts in timesheets:
        sql = text("""
            SELECT day, adj_hrs FROM timesheet_details
            WHERE timesheet_id = :tid
            ORDER BY day
        """)
        ts_details = conn.execute(sql, tid=ts.id).fetchall()

        for ts_detail in ts_details:
            x = ts.month_year
            y = datetime(x.year, x.month, ts_detail.day)
            if (y >= start_date) and (y <= end_date):
                if ts_detail.adj_hrs == None:
                    continue
                adj_hrs = Decimal('%0.2f' % ts_detail.adj_hrs)
                total_hrs += adj_hrs

    conn.close()
    return "%0.2f" % total_hrs	

@task
def get_dates(sid, userid, leads_id, DATE_SEARCH):	
    
    dates=[]	
    for d in DATE_SEARCH:
	
        present = 0
        late = 0
        early_login = 0
        extra_day = 0
        flexi = 0
        absent =0
        on_leave =0
        no_schedule =0
        not_yet_working =0	
	
        #compliance	
        compliance = get_compliance_result(sid, '%s' % d, userid, leads_id, False)	
        regular_contract_hrs = compliance['work_schedule']['regular_contract_hrs']	
	
        #worked hours
        worked_hrs = get_total_log_hrs(sid, userid, d, d)	
		
        #adjusted hours 
        adj_hrs = get_total_adj_hrs(sid, d, d)		
	
        dates.append(dict(
            sid = sid,
            date = d,
            compliance = compliance['compliance'],
            staff_start_work_hour = compliance['staff_start_work_hour'].strftime('%Y-%m-%d %I:%M:%S %p'),
            regular_contract_hrs = regular_contract_hrs,
            worked_hrs = worked_hrs,
            adj_hrs = adj_hrs 			
            )
        )
    return dates		
	
@task(ignore_result=True)	
def process_doc_id(doc_id):
    logging.info('checking %s' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
		
    start_date = datetime.strptime(doc['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(doc['end_date'], '%Y-%m-%d')
    subcontractor_ids = doc['subcontractor_ids']
    #return subcontractor_ids	
    random_string_exists = True	
    DATE_SEARCH=[]
    while random_string_exists:		
        if 	start_date <= end_date:
            DATE_SEARCH.append('%s' % start_date.strftime('%Y-%m-%d'))
            start_date = start_date + timedelta(days=1)
            			
            random_string_exists = True
        else:
            random_string_exists = False	
    data=[]
    couch_result = {}
    total_reg_hours= {}	
    total_log_hours ={}
    total_adj_hours ={}
    total_login_types={}  
    worked_hrs_rate_percentage={}	
    adjusted_hrs_rate_percentage={}
	
    tasks=[]	
    for sid in subcontractor_ids:
        sql = text("""SELECT userid, leads_id FROM subcontractors WHERE id = :sid """)
        conn = engine.connect()
        subcon = conn.execute(sql, sid=sid).fetchone()
       
        t = get_dates(int(sid), int(subcon.userid), int(subcon.leads_id), DATE_SEARCH)
        tasks.append(t)  	
		
     
    job = TaskSet(tasks = tasks)
    #result = job.apply_async()
    #data = result.join()
	
    couch_result = {}
    for i in range(len(subcontractor_ids)):
        couch_result[int(subcontractor_ids[i])] = job[i]		
    
    doc = db.get(doc_id)
    doc['result'] = couch_result
    doc['result_date_time'] = get_ph_time().strftime('%Y-%m-%d %H:%M:%S')
    db.save(doc)	
	
def run_tests():
    """
    >>> process_doc_id('e10e2d3554f64872b5fb53f0ad001d91') 	
    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
