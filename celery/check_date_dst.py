#First Sunday of October at 2 AM 
#First Sunday of April at 2 AM 

from sqlalchemy.sql import text
from persistent_mysql_connection import engine

from celery.task import task, Task
from celery.execute import send_task

from pytz import timezone
from datetime import date, timedelta, datetime
from celery.execute import send_task

import settings

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


utc_tz = timezone('UTC')
ph_tz = timezone('Asia/Manila')
now = utc_tz.localize(datetime.utcnow()).astimezone(ph_tz)

months=[4, 10] #Involved dst months "April" and "October" 
def scheduled_date(year, months, TEST):
    if TEST:
        day = now.strftime('%d')
    else:
        day = 1	
    dates=[]
    for m in months:
        d = date(int(year), int(m), int(day))
        if not TEST:		
            d +=timedelta(days= 6 - d.weekday()) # First Sunday of the month
        dates.append('%s' % d)
    return dates

@task(ignore_result=True)
def check_date(TEST):	
    #print scheduled_date(now.year, months, TEST)
    current_date = '%s' % now.strftime('%Y-%m-%d')
    #print current_date
    if current_date in scheduled_date(now.year, months, TEST):
        #print 'meron'
        conn = engine.connect()	
        sql = text("""SELECT id, work_days, mon_start, tue_start, wed_start, thu_start, fri_start, sat_start, sun_start FROM subcontractors WHERE status IN ('ACTIVE', 'suspended') 
            AND client_timezone IN (
               'Australia/Adelaide', 
               'Australia/Brisbane', 
               'Australia/Broken_Hill', 
               'Australia/Currie', 
               'Australia/Darwin', 
               'Australia/Eucla', 
               'Australia/Hobart', 
               'Australia/Lindeman', 
               'Australia/Lord_Howe', 
               'Australia/Melbourne', 
               'Australia/Perth', 
               'Australia/Sydney'
               )
 		   
            """)	
        subcons = conn.execute(sql).fetchall()
        #print '%s no. of subcons' % len(subcons)	

        subcons_regular_sched = [] #The staff has a thesame schedule everyday

	
        for s in subcons:
            staff_start_time=[]	
            if s.work_days:
                work_days = s.work_days.split(',')
            for w in work_days:		
                dayname_str = '%s_start' % w		
                staff_start_time.append('%s' % s[dayname_str])

            myset = set(staff_start_time)
            if len(myset) == 1:
                subcons_regular_sched.append(int(s.id))
				
        conn.close()	
	
	
        #send message to celery
        ph_tz = timezone('Asia/Manila')
        eta = datetime(int(now.year),int(now.month),int(now.day),2,0,0, tzinfo=ph_tz)
    

        #print '%s no. of involved contract with regular schedule.' % len(subcons_regular_sched)
        #print subcons_regular_sched	
        #send_task('staff_working_time.dst_time_update', [subcons_regular_sched], eta=eta)		


	

if __name__ == '__main__':
    #send_task("check_date_dst.check_date", [True])	
    logging.info('tests')
    logging.info(check_date(True))    