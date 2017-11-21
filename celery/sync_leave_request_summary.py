#!/usr/bin/env python
#   2015-01-26  Normaneil Macutay <normanm@remotestaff.com.au>
#   -   initial commit
#   -   task to sync mongogdb.reports.leave_request_summary
#   -   creating mongodb document
#   -   a script to get all recorded years saved in remotestaff.leave_request_dates table
#   -   a script to get the total number of leave request filed per year depending on the date of leave.



from sqlalchemy.sql import text
from persistent_mysql_connection import engine

from celery.task import task, Task
from celery.execute import send_task

from datetime import date, datetime, timedelta
import pytz
from pytz import timezone

from decimal import Decimal


import couchdb

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import settings

from pymongo import MongoClient
from bson.objectid import ObjectId
    
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
               
    
@task(ignore_result=True)
def process():
    logging.info('from sc.remotestaff.com.au executing sync_leave_request_summary.process %s' % get_ph_time() )
    now = get_ph_time()
    start_year = 2008
    current_year =int(now.strftime('%Y'))
    
    if settings.DEBUG:        
        try:
            mongo_client = MongoClient()
        except:
            mongo_client = MongoClient(host=settings.MONGO_TEST)        
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD)
        
    conn = engine.connect()

    #get all recorded years
    sql = "SELECT DISTINCT(YEAR(d.date_of_leave))AS year FROM leave_request_dates d WHERE d.status NOT IN('cancelled', 'absent');"
    years = conn.execute(sql).fetchall()

    data=[]
    for year in years:
        year = '%d' % year[0]
        year = int(year)
        data.append(dict(year = year))
    
        
    #return data
    #connect to the test db
    db = mongo_client.reports
    #retrieve the person collection
    col = db.leave_request_summary

    #save first the recorded years
    mongo_cursor = col.find_one({'summary_type' : 'recorded years'}) 
    #return mongo_cursor["_id"]
    
    try:
        if mongo_cursor!=None:
            col.update({"_id" : ObjectId(mongo_cursor["_id"])}, {"$set":{'recorded_years' : data}})
            col.update({"_id" : ObjectId(mongo_cursor["_id"])}, {"$set":{'last_synced' : '%s' % now}})
            doc_id = mongo_cursor["_id"]
        else:
            record={'summary_type' : 'recorded years', 'recorded_years' : data, 'date_created' : '%s' % now}
            doc_id = col.insert(record)
    except:
        pass
    
    
    
    #return doc_id
    str = ""
    for d in data:
        
        #str += '%s\n' % d['year']
        year = int(d['year'])
        
        mongo_cursor = col.find_one({'year':year, 'summary_type' : 'total per year'})  
        try:
            if mongo_cursor!=None:
                if year >= current_year:
                    sql = "SELECT l.id FROM leave_request l JOIN leave_request_dates d ON d.leave_request_id = l.id WHERE l.leave_type NOT IN('Absent') AND YEAR(d.date_of_leave) = '%s' AND d.status NOT IN('cancelled', 'absent') GROUP BY l.id" % year
                    records = conn.execute(sql).fetchall()
                    leave_requests=[]
                    for rec in records:
                        leave_requests.append(dict(leave_request_id = int(rec['id'])))
                        
                    col.update({'year': year }, {"$set":{'records' : leave_requests}})    
                    col.update({'year': year }, {"$set":{'total' : len(records)}})
                    col.update({'year': year }, {"$set":{'last_synced' : '%s' % now}})
                    col.update({'year': year }, {"$set":{'updated' : True}})
            else:
                
                sql = "SELECT l.id FROM leave_request l JOIN leave_request_dates d ON d.leave_request_id = l.id WHERE l.leave_type NOT IN('Absent') AND YEAR(d.date_of_leave) = '%s' AND d.status NOT IN('cancelled', 'absent') GROUP BY l.id" % year
                records = conn.execute(sql).fetchall()
                leave_requests=[]
                for rec in records:
                    leave_requests.append(dict(leave_request_id = int(rec['id'])))
                
                record={'year' : year, 'total' : len(records), 'summary_type' : 'total per year', 'date_created' : '%s' % now, 'updated' : False, 'records' : leave_requests}
                col.insert(record)
        except:
            pass



    #leave request filed today
    mongo_cursor = col.find_one({'summary_type' : 'on leave today'}) 
    try:
        if mongo_cursor!=None:
            sql = "SELECT l.id, d.status FROM leave_request l JOIN leave_request_dates d ON d.leave_request_id = l.id WHERE l.leave_type NOT IN('Absent') AND DATE(d.date_of_leave) = '%s' AND d.status NOT IN('cancelled', 'absent') GROUP BY l.id" % now.strftime('%Y-%m-%d')
            records = conn.execute(sql).fetchall()
            leave_requests=[]
            for rec in records:
                leave_requests.append(dict(
                    leave_request_id = int(rec['id']),
                    status = rec['status'],
                    )
                )
                
            col.update({"_id" : ObjectId(mongo_cursor["_id"])}, {"$set":{'last_synced' : '%s' % now}})
            col.update({"_id" : ObjectId(mongo_cursor["_id"])}, {"$set":{'records' : leave_requests}})
            col.update({"_id" : ObjectId(mongo_cursor["_id"])}, {"$set":{'total' : len(leave_requests) }})
        else:
            sql = "SELECT l.id, d.status FROM leave_request l JOIN leave_request_dates d ON d.leave_request_id = l.id WHERE l.leave_type NOT IN('Absent') AND DATE(d.date_of_leave) = '%s' AND d.status NOT IN('cancelled', 'absent') GROUP BY l.id" % now.strftime('%Y-%m-%d')
            records = conn.execute(sql).fetchall()
            leave_requests=[]
            for rec in records:
                leave_requests.append(dict(
                    leave_request_id = int(rec['id']),
                    status = rec['status'],
                    )
                )
                
            record={'summary_type' : 'on leave today', 'total' : 'initial test', 'date_created' : '%s' % now, 'records' : leave_requests, 'total' : len(leave_requests)}
            col.insert(record)
    except:
        pass
            
            
    conn.close()     
    #return str
				
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process())