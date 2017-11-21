#   2014-07-08  Normaneil E. Macutay <normanm@remotestaff.com.au>
#   -   task to get the total log hours of staff based on start and end date

#   2015-09-15  Normaneil E. Macutay <normanm@remotestaff.com.au>
#   -   Removed dependency to persistent mysql connection to prevent error "Mysql has Gone Away".


import settings
import couchdb
import MySQLdb
from celery.task import task, Task
from celery.execute import send_task
from celery.task.sets import TaskSet
from datetime import date, datetime, timedelta
import pytz
from pytz import timezone
from decimal import Decimal, ROUND_HALF_UP
import calendar
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
from pymongo import MongoClient
from bson.objectid import ObjectId
TWOPLACES = Decimal(10) ** -2


def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
    
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
def get_total_adj_hrs_per_month(conn, sid, date_search):
    str=""
    data={}
    for d in date_search:
        d = datetime.strptime(d, '%Y-%m-%d')  
        end_date = datetime(d.year, d.month, calendar.mdays[d.month])
        
        #str += '%s %s' % (d.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        total_adj_hrs = get_total_adj_hrs(conn, sid, d, end_date)
        data['%s' % d.strftime('%m/%Y')] = total_adj_hrs
    return data    

    
@task    
def get_total_adj_hrs(conn, sid, start_date, end_date):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
    
    total_hrs = Decimal('0.00')
    x = start_date
    start_date_ts = date(x.year, x.month, 1)

    #get all involved timesheet
    sql = "SELECT id, month_year FROM timesheet WHERE status IN ('open', 'locked') AND subcontractors_id=%s AND month_year BETWEEN '%s' AND '%s' ;" % (sid, start_date_ts, end_date )
    conn.execute(sql)
    timesheets = dictfetchall(conn)

    for ts in timesheets:
        sql = "SELECT day, adj_hrs FROM timesheet_details WHERE timesheet_id=%s ORDER BY day " % ts['id']
        conn.execute(sql)
        ts_details = dictfetchall(conn)
        

        for ts_detail in ts_details:
            x = ts['month_year']
            y = datetime(x.year, x.month, ts_detail['day'])
            if (y >= start_date) and (y <= end_date):
                if ts_detail['adj_hrs'] == None:
                    continue
                adj_hrs = Decimal('%0.2f' % ts_detail['adj_hrs'])
                total_hrs += adj_hrs


    return "%0.2f" % total_hrs
    
@task    
def get_subcon_rates(conn, sid, start_date, end_date):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
    
    rates=[]
    str=""
    data=[]
    
    
    #get all involved timesheet
    sql = "SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate WHERE subcontractors_id=%s AND start_date BETWEEN '%s' AND '%s' " % (sid, start_date, end_date)    
    rates = conn.execute(sql)
        
    if rates:
        rates = dictfetchall(conn)
        for r in rates:
            end_date=None
            client_price = 0.00
            if r["end_date"]:
                end_date = r["end_date"]
                
            if r["rate"]:
                client_price = r["rate"]
                
            data.append(dict(
                rate = client_price,
                start_date = r["start_date"],
                end_date = end_date,
                work_status = r["work_status"]
                )
            )
    
    if not rates:
        sql = "SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate WHERE subcontractors_id=%s AND DATE(start_date) < '%s' ORDER BY start_date DESC LIMIT 1" % (sid,  start_date)
        rate = conn.execute(sql)
        
            
        if rate:
            rate = dictfetchall(conn)
            rate = rate[0]
            
            end_date=None
            client_price = 0.00
            if rate["end_date"]:
                end_date = rate["end_date"]
                
            if rate["rate"]:
                client_price = rate["rate"]
                
            data.append(dict(
                rate = client_price,
                start_date = rate["start_date"],
                end_date = end_date,
                work_status = rate["work_status"]
                )
            )
    
    return data

def get_subcon_rate(conn, sid, start_date, ):
    
    sql = "SELECT id, start_date, end_date, rate, work_status FROM subcontractors_client_rate WHERE subcontractors_id=%s AND DATE(start_date) < '%s'  ORDER BY start_date DESC LIMIT 1;" % (sid, start_date)
    
    rates = conn.execute(sql)
    if rates:
        rates = dictfetchall(conn)
        #rates = rates[0]
    
    return rates

def get_rates_reference_date(conn,  sid, start_date, end_date):

    
    
    sql = "SELECT start_date, end_date FROM subcontractors_client_rate WHERE subcontractors_id=%s AND start_date BETWEEN '%s' AND '%s';" % (sid, start_date, end_date)
    rates = conn.execute(sql)
    rates = dictfetchall(conn)
    #rates = rates[0]
    
    if not rates:
        sql = "SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate WHERE subcontractors_id=%s AND DATE(start_date) < '%s' ORDER BY start_date DESC LIMIT 1;" % (sid, start_date)
        rates = conn.execute(sql)
        rates = dictfetchall(conn)
        #rates = rates[0]
    
    
    
    
    return rates

    
def get_subcon_details(conn, sid):
    sql = "SELECT status, starting_date, end_date, work_status FROM subcontractors WHERE id=%s;" % sid
    subcon = conn.execute(sql)
    subcon = dictfetchall(conn)
    subcon = subcon[0]
    
    return subcon
    
@task(ignore_result=True)
def process_doc_id(doc_id):
    import calendar
    logging.info('soa.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    
        
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
        
    subcontractor_ids = doc['subcontractor_ids']    
    date_search = doc['date_search']    
    
            
    database = MySQLdb.connect(**settings.DB_ARGS)
    conn = database.cursor()

    
    str="\n"
    result={}
    subcon_client_rates={}
    for d in date_search:
        d = datetime.strptime(d, '%Y-%m-%d')  
        #end_date = datetime(d.year, d.month, calendar.mdays[d.month])
        last_day_of_end_date = calendar.monthrange(int(d.year), int(d.month))
        end_date = datetime(d.year, d.month, last_day_of_end_date[1])
        date_reference = '%s' % d.strftime('%m/%Y')
        
        subcons=[]
        client_rates=[]
        for sid in subcontractor_ids:
            userid =  doc['subcon_userid'][sid]
            
            dates = get_rates_reference_date(conn, sid, d, end_date)
            #str += '\n\n%s %s' % (d, end_date)
            
            subcon = get_subcon_details(conn, sid)
            starting_date = datetime.strptime('%s' % subcon["starting_date"], '%Y-%m-%d')
            
            
            rates=[]
            
            for idx, date in enumerate(dates):
                #str += '\n\t%s' % (date['start_date'])
                #print str
                start_date_ref = dates[idx]['start_date']
                
                if date_reference != '%s' % date['start_date'].strftime('%m/%Y'):
                    #str += '\n\t%s use this date => %s' % (date['start_date'], d)
                    rates = get_subcon_rate(conn, sid, d)
                    if rates:
                        for r in rates:
                            #str += '\n\n\t->%s %s %s %s\n' % (r['rate'], r['work_status'] , d, end_date )
                            client_rates.append(dict(                                
                                rate = r['rate'],
                                work_status = r['work_status'],
                                start_date =  d,
                                end_date = end_date,
                                sid = sid
                                )
                            )
                        
                        
                        
                if date_reference == '%s' % date['start_date'].strftime('%m/%Y'):         
                    
                    end_date_ref = dates[idx]['start_date'] - timedelta(days=1)
                    start_date_ref = dates[idx-1]['start_date']
                    
                    same_month = True
                    if '%s' % dates[idx]['start_date'].strftime('%m/%Y') != '%s' % end_date_ref.strftime('%m/%Y'):
                        #end_date_ref =  dates[idx]['start_date']
                        same_month = False    
                    if same_month:
                        if int(idx) == 0:                        
                            #str += '\n\t%s %s %s' % (idx, d, end_date_ref)
                            rates = get_subcon_rate(conn, sid, end_date_ref)
                            if rates:
                                for r in rates:
                                    #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                    client_rates.append(dict(
                                        rate = r['rate'],
                                        work_status = r['work_status'],
                                        start_date = d,
                                        end_date = end_date_ref,
                                        sid = sid
                                        )
                                    )
                        
                    if int(idx) > 0:                            
                        #str += '\n\t%s %s %s' % (idx, start_date_ref, end_date_ref)                        
                        rates = get_subcon_rate(conn, sid, end_date_ref)
                        if rates:
                            for r in rates:
                                #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                client_rates.append(dict(
                                    rate = r['rate'],
                                    work_status = r['work_status'],
                                    start_date = start_date_ref,
                                    end_date = end_date_ref,
                                    sid = sid
                                    )
                                )
                    if len(dates) == (idx+1):
                        #str += '\n\t%s %s %s' % ((idx+1), dates[idx]['start_date'], end_date)
                        rates = get_subcon_rate(conn, sid, end_date)
                        if rates:
                            for r in rates:
                                #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                client_rates.append(dict(
                                    rate = r['rate'],
                                    work_status = r['work_status'],
                                    start_date = dates[idx]['start_date'],
                                    end_date = end_date,
                                    sid = sid
                                    )
                                )                
                                
                                
                
            #subcon_client_rates[int(sid)]=client_rates
            for c in client_rates:        
                
                start_date_ref = datetime.strptime('%s' % c['start_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')
                end_date_ref = datetime.strptime('%s' % c['end_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')    
                adj_hrs = get_total_adj_hrs(conn, sid, start_date_ref, end_date_ref)
                
                if date_reference == '%s' % c['start_date'].strftime('%m/%Y'):
                    if sid == c['sid']:
                        str += '\n[%s] %s %s %s %s' % (sid, c['start_date'].strftime("%Y-%m-%d"), c['end_date'].strftime("%Y-%m-%d"), c['rate'], c['work_status'])
                        if subcon["status"] == 'ACTIVE' or subcon["status"] == 'suspended' :
                            if starting_date <= end_date:     
                                subcons.append(dict(
                                    sid = sid,
                                    rate = c['rate'],
                                    work_status = c['work_status'],
                                    start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                    end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                    total_adj_hrs = '%s' % adj_hrs
                                    )
                                )
                            
            
                        if subcon["status"] == 'terminated' or subcon["status"] == 'resigned' :
                            if subcon["end_date"]:                 
                                ending_date = subcon["end_date"]            
                                if starting_date <= end_date:
                                    if ending_date >= d and ending_date >= end_date:             
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"), 
                                            total_adj_hrs = '%s' % adj_hrs    
                                            )
                                        )
                                            
                                                        
                                    if ending_date >= d and ending_date <= end_date:                                                
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                            total_adj_hrs = '%s' % adj_hrs        
                                            )
                                        )
                                        
            result['%s' % d.strftime('%Y-%m-%d')] = subcons                            
    #return str
    doc['result'] = result 
    doc['result_date_time'] = get_ph_time().strftime('%Y-%m-%d %H:%M:%S')    
    conn.close()        
    db.save(doc)
    
    
@task(ignore_result=True)
def build_mass_soa(doc_id):
    logging.info('soa.build_mass_soa checking %s' % doc_id)
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)
        #mongo_client = MongoClient()
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017)
        
    now = get_ph_time(as_array=False)    
        
    #connect to the test db
    db = mongo_client.accounts
    #retrieve the person collection
    col = db.soa
    
    
    try:
        doc = col.find_one({"_id" : ObjectId(doc_id)})
    except:
        return 'invalid mongodb document id'
            
    if not doc:
        return 'mongodb document id not found'
    #return doc['start_date'].strftime('%Y-%m-%d')
    str=""
    
    database = MySQLdb.connect(**settings.DB_ARGS)
    conn = database.cursor()
    
    results=[]
    date_search = doc['date_search']
    #return len(doc['client_ids'])
    for client in doc['client_ids']:
        #str+= '%s\n' %  client['leads_id']
        leads_id = int(client['leads_id'])
        currency_couch = client['currency_couch']
        subcons = client['subcons']
        apply_gst_couch = client['apply_gst_couch']
        
        result=[]
        if subcons:
            result = build_soa_per_client(leads_id, date_search, subcons, doc_id, conn)
            
        results.append(dict(
            leads_id = leads_id,
            currency_couch = currency_couch,
            result = result,
            apply_gst_couch = apply_gst_couch
        ))
        
        
        #get all subcons of client
    #conn.close()
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'results' : results}})
    
    #update the document assuming there's no issue occured.
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'status' : "executed" }})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'message' : "finish generating soa for all clients" }})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'date_finished' : now }})
    
    #TODO
    #ALTER TABLE `statement_of_accounts` ADD COLUMN `reference_db` ENUM('couchdb','mongodb') NOT NULL DEFAULT 'couchdb' COMMENT 'determines where the document saved';
    
    #new cursor for updating and insertion. As per Allan advice
    db_update = MySQLdb.connect(**settings.DB_ARGS)
    cursor = db_update.cursor()
    
    cursor.execute("set autocommit = 1")
        
    sql = "INSERT INTO statement_of_accounts (doc_id, admin_id, date_created, start_date, end_date, reference_db) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (doc_id, doc["admin_id"], now, doc['start_date'].strftime('%Y-%m-%d'), doc['end_date'].strftime('%Y-%m-%d'), 'mongodb')
    cursor.execute(sql)
    #close mysql connections
    db_update.commit()    
    cursor.close()
    
    
    #Finally removed the build document. Signalling that the system is ready for another mass soa generation
    try:
        col.remove({"_id" : "build"})
    except:
        return "no build document found."
        
    logging.info('finish generating soa for all clients mongodb_doc_id %s' % doc_id)
    #return doc['message']

    

def build_soa_per_client(leads_id, date_search, subcontractor_ids, doc_id, conn):
    if settings.DEBUG:
        mongo_client = MongoClient(host=settings.MONGO_TEST, port=27017)
    else:
        mongo_client = MongoClient(host=settings.MONGO_PROD, port=27017)
           
    #connect to the test db
    db = mongo_client.accounts
    #retrieve the person collection
    col = db.soa
    
    doc = col.find_one({"_id" : ObjectId(doc_id)})
    col.update({"_id" : ObjectId(doc_id)}, {"$set":{'message' : "building soa for client id %s" % leads_id}})
    logging.info('building soa for client id=> %s doc_id=>%s ' % (leads_id, doc_id) )
    results=[]
    for d in date_search:
        d = datetime.strptime(d, '%Y-%m-%d')  
        end_date = datetime(d.year, d.month, calendar.mdays[d.month])
        date_reference = '%s' % d.strftime('%m/%Y')
        
        subcons=[]
        client_rates=[]
        result=[]
        for subcon in subcontractor_ids:
            sid = int(subcon['subcon_id'])
            
            dates = get_rates_reference_date(conn, sid, d, end_date)
                      
            subcon = get_subcon_details(conn, sid)
            starting_date = datetime.strptime('%s' % subcon["starting_date"], '%Y-%m-%d')
            
            
            rates=[]
            #configure the rates by date
            for idx, date in enumerate(dates):
                #str += '\n\t%s' % (date['start_date'])
                start_date_ref = dates[idx]['start_date']
                
                if date_reference != '%s' % date['start_date'].strftime('%m/%Y'):
                    #str += '\n\t%s use this date => %s' % (date['start_date'], d)
                    rates = get_subcon_rate(conn, sid, d)
                    if rates:
                        for r in rates:
                            #str += '\n\n\t->%s %s %s %s\n' % (r['rate'], r['work_status'] , d, end_date )
                            client_rates.append(dict(                                
                                rate = r['rate'],
                                work_status = r['work_status'],
                                start_date =  d,
                                end_date = end_date,
                                sid = sid
                                )
                            )
                        
                        
                        
                if date_reference == '%s' % date['start_date'].strftime('%m/%Y'):         
                    
                    end_date_ref = dates[idx]['start_date'] - timedelta(days=1)
                    start_date_ref = dates[idx-1]['start_date']
                    
                    same_month = True
                    if '%s' % dates[idx]['start_date'].strftime('%m/%Y') != '%s' % end_date_ref.strftime('%m/%Y'):
                        #end_date_ref =  dates[idx]['start_date']
                        same_month = False    
                    if same_month:
                        if int(idx) == 0:                        
                            #str += '\n\t%s %s %s' % (idx, d, end_date_ref)
                            rates = get_subcon_rate(conn, sid, end_date_ref)
                            if rates:
                                for r in rates:
                                    #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                    client_rates.append(dict(
                                        rate = r['rate'],
                                        work_status = r['work_status'],
                                        start_date = d,
                                        end_date = end_date_ref,
                                        sid = sid
                                        )
                                    )
                        
                    if int(idx) > 0:                            
                        #str += '\n\t%s %s %s' % (idx, start_date_ref, end_date_ref)                        
                        rates = get_subcon_rate(conn, sid, end_date_ref)
                        if rates:
                            for r in rates:
                                #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                client_rates.append(dict(
                                    rate = r['rate'],
                                    work_status = r['work_status'],
                                    start_date = start_date_ref,
                                    end_date = end_date_ref,
                                    sid = sid
                                    )
                                )
                    if len(dates) == (idx+1):
                        #str += '\n\t%s %s %s' % ((idx+1), dates[idx]['start_date'], end_date)
                        rates = get_subcon_rate(conn, sid, end_date)
                        if rates:
                            for r in rates:
                                #str += '\n\t\t->%s %s' % (r['rate'], r['work_status'])
                                client_rates.append(dict(
                                    rate = r['rate'],
                                    work_status = r['work_status'],
                                    start_date = dates[idx]['start_date'],
                                    end_date = end_date,
                                    sid = sid
                                    )
                                )
            
            for c in client_rates:        
                
                start_date_ref = datetime.strptime('%s' % c['start_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')
                end_date_ref = datetime.strptime('%s' % c['end_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')    
                adj_hrs = get_total_adj_hrs(conn, sid, start_date_ref, end_date_ref)
                
                if date_reference == '%s' % c['start_date'].strftime('%m/%Y'):
                    if sid == c['sid']:
                        #str += '\n[%s] %s %s %s %s' % (sid, c['start_date'].strftime("%Y-%m-%d"), c['end_date'].strftime("%Y-%m-%d"), c['rate'], c['work_status'])
                        if subcon["status"] == 'ACTIVE' or subcon["status"] == 'suspended' :
                            if starting_date <= end_date:     
                                subcons.append(dict(
                                    sid = sid,
                                    rate = c['rate'],
                                    work_status = c['work_status'],
                                    start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                    end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                    total_adj_hrs = '%s' % adj_hrs
                                    )
                                )
                            
            
                        if subcon["status"] == 'terminated' or subcon["status"] == 'resigned' :
                            if subcon["end_date"]:                 
                                ending_date = subcon["end_date"]            
                                if starting_date <= end_date:
                                    if ending_date >= d and ending_date >= end_date:             
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"), 
                                            total_adj_hrs = '%s' % adj_hrs    
                                            )
                                        )
                                            
                                                        
                                    if ending_date >= d and ending_date <= end_date:                                                
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                            total_adj_hrs = '%s' % adj_hrs        
                                            )
                                        )        

            
            #result.append(dict(
            #    subcons = subcons,                    
            #))          
            #result.append(dict(
            #    date_reference = date_reference,
            #    subcons = '%s %s' % (sid, subcon['status'])
            #))
                                        
        
        
        results.append(dict(            
            date_refernce = date_reference,
            subcons = subcons
        ))
        
                        
    return results    


    
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process_doc_id('fe13cf215bc101ba829b93fec69135c8'))
    #logging.info(build_mass_soa('558e544a6e955275b7372e19'))
