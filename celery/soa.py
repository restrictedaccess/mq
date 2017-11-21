#   2014-07-08  Normaneil E. Macutay <normanm@remotestaff.com.au>
#   -   task to get the total log hours of staff based on start and end date


import settings
import couchdb


from persistent_mysql_connection import engine
from sqlalchemy.sql import text

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
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


TWOPLACES = Decimal(10) ** -2

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
    #return '%s %s' % (start_date_ts, end_date)
    #get all involved timesheet
    sql = text("""
        SELECT id, month_year FROM timesheet
        WHERE status IN ('open', 'locked')
        AND subcontractors_id = :sid
        AND month_year BETWEEN :start_date AND :end_date
    """)
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


    return Decimal('%0.2f' % total_hrs)
    
@task    
def get_subcon_rates(conn, sid, start_date, end_date):
    """
    conn, a mysql connector, was also passed to ease out connect/disconnects
    """
    
    rates=[]
    str=""
    data=[]
    
    
    #get all involved timesheet
    sql = text("""
        SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate
        WHERE subcontractors_id = :sid
        AND start_date BETWEEN :start_date AND :end_date
    """)
    rates = conn.execute(sql, sid=sid, start_date=start_date, end_date=end_date).fetchall()
    if rates:
        for r in rates:
            end_date=None
            client_price = 0.00
            if r.end_date:
                end_date = r.end_date
                
            if r.rate:
                client_price = r.rate
                
            data.append(dict(
                rate = client_price,
                start_date = r.start_date,
                end_date = end_date,
                work_status = r.work_status
                )
            )
    
    if not rates:
        sql = text("""
            SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate
            WHERE subcontractors_id = :sid
            AND start_date < :start_date 
            ORDER BY start_date DESC LIMIT 1
        """)
        rate = conn.execute(sql, sid=sid, start_date=start_date).fetchone()
        if rate:
            end_date=None
            client_price = 0.00
            if rate.end_date:
                end_date = rate.end_date
                
            if rate.rate:
                client_price = rate.rate
                
            data.append(dict(
                rate = client_price,
                start_date = rate.start_date,
                end_date = end_date,
                work_status = rate.work_status
                )
            )
    
    return data

def get_subcon_rate(conn, sid, start_date, ):
    
    sql = text("""
        SELECT id, start_date, end_date, rate, work_status FROM subcontractors_client_rate
        WHERE subcontractors_id = :sid
        AND start_date < :start_date 
        ORDER BY start_date DESC LIMIT 1
    """)
    rates = conn.execute(sql, sid=sid, start_date=start_date).fetchall()
    return rates

def get_rates_reference_date(conn,  sid, start_date, end_date):

    
    
    sql = text("""
        SELECT start_date, end_date FROM subcontractors_client_rate
        WHERE subcontractors_id = :sid
        AND start_date BETWEEN :start_date AND :end_date
    """)
    rates = conn.execute(sql, sid=sid, start_date=start_date, end_date=end_date).fetchall()

    
    if not rates:
        sql = text("""
            SELECT start_date, end_date, rate, work_status FROM subcontractors_client_rate
            WHERE subcontractors_id = :sid
            AND start_date < :start_date 
            ORDER BY start_date DESC LIMIT 1
        """)
        rates = conn.execute(sql, sid=sid, start_date=start_date).fetchall()
    
    
    
    
    return rates

    
def get_subcon_details(conn, sid):
    sql = text("""
        SELECT status, starting_date, end_date, work_status FROM subcontractors
        WHERE id = :sid
    """)
    subcon = conn.execute(sql, sid=sid).fetchone()
    return subcon
    
@task(ignore_result=True)
def process_doc_id(doc_id):
    logging.info('soa.process_doc_id checking %s from sc.remotestaff.com.au' % doc_id)
    s = couchdb.Server(settings.COUCH_DSN)
    
    #send notice to devs
    now = get_ph_time(as_array=False) 
    to=['devs@remotestaff.com.au']
    couch_mailbox = s['mailbox']    
    date_created = [now.year, now.month, now.day, now.hour, now.minute, now.second]          
    mailbox = dict(
        sent = False,        
        bcc = None,
        cc = None,
        created = date_created,
        generated_by = 'celery soa.process_doc_id',
        html = None,
        text = 'executing soa.process_doc_id %s' % doc_id,        
        subject = 'executing soa.process_doc_id %s' % doc_id,
        to = to            
    )
    mailbox['from'] = 'noreply@remotestaff.com.au'        
    couch_mailbox.save(mailbox)    
    
    db = s['subconlist_reporting']
    doc = db.get(doc_id)
    if doc == None:
        raise Exception('subconlist_reporting document not found : %s' % doc_id)
        
    subcontractor_ids = doc['subcontractor_ids']    
    date_search = doc['date_search']    
    
            
    conn = engine.connect()    
    str="\n"
    result={}
    subcon_client_rates={}
    for d in date_search:
        d = datetime.strptime(d, '%Y-%m-%d')  
        end_date = datetime(d.year, d.month, calendar.mdays[d.month])
        date_reference = '%s' % d.strftime('%m/%Y')
        
        subcons=[]
        client_rates=[]
        for sid in subcontractor_ids:
            userid =  doc['subcon_userid'][sid]
            
            dates = get_rates_reference_date(conn, sid, d, end_date)
            #str += '\n\n%s %s' % (d, end_date)
            
            subcon = get_subcon_details(conn, sid)
            starting_date = datetime.strptime('%s' % subcon.starting_date, '%Y-%m-%d')
            
            
            rates=[]
            
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
                                
                                
                                
                
            #subcon_client_rates[int(sid)]=client_rates
            for c in client_rates:        
                
                start_date_ref = datetime.strptime('%s' % c['start_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')
                end_date_ref = datetime.strptime('%s' % c['end_date'].strftime("%Y-%m-%d"), '%Y-%m-%d')    
                adj_hrs = get_total_adj_hrs(conn, sid, start_date_ref, end_date_ref)
                
                if date_reference == '%s' % c['start_date'].strftime('%m/%Y'):
                    if sid == c['sid']:
                        str += '\n[%s] %s %s %s %s' % (sid, c['start_date'].strftime("%Y-%m-%d"), c['end_date'].strftime("%Y-%m-%d"), c['rate'], c['work_status'])
                        if subcon.status == 'ACTIVE' or subcon.status == 'suspended' :
                            if starting_date <= end_date:     
                                subcons.append(dict(
                                    sid = sid,
                                    rate = c['rate'],
                                    work_status = c['work_status'],
                                    start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                    end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                    total_adj_hrs = '%s' % Decimal('%0.2f' % adj_hrs)
                                    )
                                )
                            
            
                        if subcon.status == 'terminated' or subcon.status == 'resigned' :
                            if subcon.end_date:                 
                                ending_date = subcon.end_date            
                                if starting_date <= end_date:
                                    if ending_date >= d and ending_date >= end_date:             
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"), 
                                            total_adj_hrs = '%s' % Decimal('%0.2f' % adj_hrs)    
                                            )
                                        )
                                            
                                                        
                                    if ending_date >= d and ending_date <= end_date:                                                
                                        subcons.append(dict(
                                            sid = sid,
                                            rate = c['rate'],
                                            work_status = c['work_status'],
                                            start_date = '%s' % c['start_date'].strftime("%Y-%m-%d"),
                                            end_date = '%s' % c['end_date'].strftime("%Y-%m-%d"),
                                            total_adj_hrs = '%s' % Decimal('%0.2f' % adj_hrs)        
                                            )
                                        )
                                        
            result['%s' % d.strftime('%Y-%m-%d')] = subcons                            
    #return str
    doc['result'] = result 
    doc['result_date_time'] = get_ph_time().strftime('%Y-%m-%d %H:%M:%S')    
    conn.close()        
    db.save(doc)
    
    
if __name__ == '__main__':
    logging.info('tests')
    logging.info(process_doc_id('d3b9c2988ed172fb588e274ffd003005'))
