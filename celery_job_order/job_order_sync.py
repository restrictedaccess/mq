#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level

from celery.task import task
from celery.execute import send_task
import settings
import logging

import MySQLdb
from StaffLister import StaffLister
from pymongo import MongoClient
import datetime
from celery.execute import send_task
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
    
def getPosting(gs_job_titles_details_id):
    db = MySQLdb.connect(**settings.DB_ARGS)
    c = db.cursor()
    sql = "SELECT p.id FROM posting AS p WHERE p.job_order_id = '{gs_job_titles_details_id}'".format(gs_job_titles_details_id=gs_job_titles_details_id)
    c.execute(sql)
    result = dictfetchall(c)
    if result and len(result) > 0:
        return result[0]["id"]
    else:
        send_task('skype_messaging.notify_devs', ["Job Order failed to sync: {'sql':"+sql])
                                                    
        return None
        
def getStatus(status):
    if status == "new" or status == "active" or status == None or status == "":
        return "Open"
    elif status == "cancel":
        return "Did not push through"
    elif status == "finish":
        return "Closed"
    elif status == "onhold":
        return "On Hold"
    elif status == "ontrial":
        return "On Trial"
    else:
        return status
    
def getRecruiters(job_order):
    if job_order["gs_job_titles_details_id"] and job_order["gs_job_titles_details_id"]!="":
        sql = "SELECT * FROM gs_job_orders_recruiters_links WHERE link_id={link_id} AND link_type='JO'".format(link_id=job_order["gs_job_titles_details_id"])
    else:
        sql = "SELECT * FROM gs_job_orders_recruiters_links WHERE link_id={link_id} AND link_type='Lead'".format(link_id=job_order["leads_id"])
    
    db = MySQLdb.connect(**settings.DB_ARGS)
    c = db.cursor()
    c.execute(sql)
    recruiters = dictfetchall(c)     
    return recruiters

def getLastContact(job_order):
    sql = ""
    if job_order["leads_id"] and job_order["assigned_hiring_coordinator_id"] and job_order["leads_id"]!=None and job_order["assigned_hiring_coordinator_id"]!=None:
        
        sql = 'SELECT DATE(date_created) AS last_contact FROM `history` WHERE (agent_no = "'+str(job_order["assigned_hiring_coordinator_id"])+'") AND (leads_id = "'+str(job_order["leads_id"])+'") UNION SELECT DATE(e.date_endoesed) AS last_contact FROM `tb_endorsement_history` AS `e` INNER JOIN `leads` AS `l` ON l.id = e.client_name WHERE (l.id = "'+str(job_order["leads_id"])+'") AND (l.hiring_coordinator_id = "'+str(job_order["assigned_hiring_coordinator_id"])+'") UNION SELECT CASE WHEN tbr.date_updated IS NULL THEN DATE(tbr.date_added) ELSE DATE(tbr.date_updated) END AS last_contact FROM `tb_request_for_interview` AS `tbr` INNER JOIN `leads` AS `l` ON l.id = tbr.leads_id WHERE (l.id = "'+str(job_order["leads_id"])+'") AND (l.hiring_coordinator_id = "'+str(job_order["assigned_hiring_coordinator_id"])+'") UNION SELECT CASE WHEN date_updated IS NULL THEN date_updated ELSE date_created END AS last_contact FROM job_order_comments WHERE tracking_code = "'+str(job_order["tracking_code"])+'" ORDER BY `last_contact` DESC'
        
    elif job_order["leads_id"] and job_order["leads_id"]!=None:
        
        sql = 'SELECT DATE(date_created) AS last_contact FROM `history` WHERE (leads_id = "'+str(job_order["leads_id"])+'") UNION SELECT DATE(e.date_endoesed) AS last_contact FROM `tb_endorsement_history` AS `e` INNER JOIN `leads` AS `l` ON l.id = e.client_name WHERE (l.id = "'+str(job_order["leads_id"])+'")  UNION SELECT CASE WHEN tbr.date_updated IS NULL THEN DATE(tbr.date_added) ELSE DATE(tbr.date_updated) END AS last_contact FROM `tb_request_for_interview` AS `tbr` INNER JOIN `leads` AS `l` ON l.id = tbr.leads_id WHERE (l.id = "'+str(job_order["leads_id"])+'") UNION SELECT CASE WHEN date_updated IS NULL THEN date_updated ELSE date_created END AS last_contact FROM job_order_comments WHERE tracking_code = "'+str(job_order["tracking_code"])+'" ORDER BY `last_contact` DESC'
    
    if sql!= "":
        
        if settings.DEBUG:
            db = MySQLdb.connect(**settings.DB_ARGS)
        else:
            db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        c = db.cursor()
        c.execute(sql)
        last_contacts = dictfetchall(c)
        if len(last_contacts) > 0:
            last_contact = last_contacts[0]
            return datetime.datetime(last_contact["last_contact"].year, last_contact["last_contact"].month, last_contact["last_contact"].day, 0, 0)
        else:
            return "No Last Contact"
    else:
        return "No Last Contact"

def sync_personal_to_mongo(userid):
    if settings.DEBUG:
        db = MySQLdb.connect(**settings.DB_ARGS)
    else:
        db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        
    c = db.cursor()
    c.execute("SELECT userid, lname, fname, middle_name, nick_name, gender, email, DATE_FORMAT(datecreated, '%%Y-%%m-%%d') AS datecreated , DATE_FORMAT(dateupdated, '%%Y-%%m-%%d') AS dateupdated, city, state, marital_status FROM personal WHERE userid = %s", (userid))
    personal = dictfetchall(c)
    if len(personal) > 0:
        personal = personal[0]
        
        
        
        total_remote_ready_points = 0;
        
        
        if personal["datecreated"]!=None:
            conv_date = datetime.datetime.strptime(personal["datecreated"], "%Y-%m-%d")
            personal["datecreated"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
        if personal["dateupdated"]!=None:
            conv_date_updated = datetime.datetime.strptime(personal["dateupdated"], "%Y-%m-%d")
            personal["dateupdated"] = datetime.datetime(conv_date_updated.year, conv_date_updated.month, conv_date_updated.day, 0, 0)
        
        c.execute("SELECT time_zone, s.time, s.type FROM staff_selected_timezones AS s WHERE userid = %s", (personal["userid"]))
        selected_timezones = dictfetchall(c)
        
        staff_selected_timezones = []
        for selected_timezone in selected_timezones:
            td = selected_timezone["time"]
            selected_timezone["time"] = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
            staff_selected_timezones.append(selected_timezone)
        
        personal["selected_timezones"] = staff_selected_timezones
        
        
        c.execute("SELECT skill, experience, proficiency FROM skills WHERE userid = %s", (personal["userid"]))
        skills = dictfetchall(c)
        personal["skills"] = skills
        c.execute("SELECT i.type AS inactive_type, DATE_FORMAT(date, '%%Y-%%m-%%d') AS date_inactive FROM inactive_staff AS i WHERE userid = %s", (personal["userid"]))
        inactive = dictfetchall(c)
        
        inactives = []
        for inactive_item in inactive:
            if inactive_item["date_inactive"]!=None:
                conv_date = datetime.datetime.strptime(inactive_item["date_inactive"], "%Y-%m-%d")
                inactive_item["date_inactive"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
                inactives.append(inactive_item)
            
        personal["inactive"] = inactives
        c.execute("SELECT comments FROM evaluation_comments WHERE userid = %s",(personal["userid"]))
        evaluation_comments = dictfetchall(c)
        personal["evaluation_comments"] = evaluation_comments
        
        
        c.execute("SELECT freshgrad, years_worked, months_worked, intern_status, iday, imonth, iyear, intern_notice, available_status, available_notice, DATE_FORMAT(STR_TO_DATE(CONCAT(cj.amonth, ' ', cj.aday, ', ', cj.ayear), '%%M %%d, %%Y'), '%%Y-%%m-%%d') AS available_month, salary_currency, expected_salary, expected_salary_neg, companyname, position, monthfrom, yearfrom, monthto, yearto, duties, companyname2, position2, monthfrom2, yearfrom2, monthto2, yearto2, duties2, companyname3, position3, monthfrom3, yearfrom3, monthto3, yearto3, duties3, companyname4, position4, monthfrom4, yearfrom4, monthto4, yearto4, duties4, cj.latest_job_title, companyname5, position5, monthfrom5, yearfrom5, monthto5, yearto5, duties5, companyname6, position6, monthfrom6, yearfrom6, monthto6, yearto6, duties6, companyname7, position7, monthfrom7, yearfrom7, monthto7, yearto7, duties7, companyname8, position8, monthfrom8, yearfrom8, monthto8, yearto8, duties8, companyname9, position9, monthfrom9, yearfrom9, monthto9, yearto9, duties9, companyname10, position10, monthfrom10, yearfrom10, monthto10, yearto10, duties10, position_first_choice, position_first_choice_exp, position_second_choice, position_second_choice_exp, position_third_choice, position_third_choice_exp, other_choice FROM currentjob AS cj WHERE userid = %s", (personal["userid"]))
        currentjob = dictfetchall(c)
        if len(currentjob) > 0:
            currentjob = currentjob[0]
            try:
                if currentjob["available_month"]!=None:
                    conv_date = datetime.datetime.strptime(currentjob["available_month"], "%Y-%m-%d")
                    currentjob["available_month"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
            except:
                currentjob["available_month"] = None 
            personal["currentjob"] = currentjob
        else:
            personal["currentjob"] = None
        
        c.execute("SELECT work_parttime, work_fulltime, work_freelancer, expected_minimum_salary, fulltime_sched, parttime_sched FROM evaluation WHERE userid = %s", (personal["userid"]))
        evaluation = dictfetchall(c)
        if len(evaluation) > 0:
            evaluation = evaluation[0]
            personal["evaluation"] = evaluation
        else:
            personal["evaluation"] = None
        
        c.execute("SELECT rs.admin_id, a.admin_fname, a.admin_lname FROM recruiter_staff AS rs LEFT JOIN admin AS a ON a.admin_id = rs.admin_id WHERE rs.userid = %s", (personal["userid"]))
        assigned_recruiter = dictfetchall(c)
        if len(assigned_recruiter) > 0:
            assigned_recruiter = assigned_recruiter[0]
            personal["assigned_recruiter"] = assigned_recruiter
        else:
            personal["assigned_recruiter"] = None
            
            
        c.execute("SELECT sc.leads_id as lead_id,l.fname AS lead_fname,l.lname AS lead_lname,sc.status,DATE_FORMAT(starting_date, '%%Y-%%m-%%d') as cdate, DATE_FORMAT(sc.resignation_date, '%%Y-%%m-%%d') as resig_date, DATE_FORMAT(sc.date_terminated, '%%Y-%%m-%%d') as term_date FROM subcontractors as sc LEFT JOIN leads as l on sc.leads_id= l.id WHERE userid =  %s ORDER BY starting_date DESC", (personal["userid"]))
        contracts = dictfetchall(c)
        personal["rs_employment_history"] = contracts
        
        c.execute("SELECT userid AS userid, client_name AS leads_id, position AS posting_id, job_category, id FROM tb_endorsement_history WHERE userid =  %s ORDER BY date_endoesed DESC", (personal["userid"]))
        endorsements = dictfetchall(c)
        personal["endorsements"] = endorsements
        
        c.execute("SELECT userid AS userid, position AS posting_id, status AS status, DATE_FORMAT(date_listed, '%%Y-%%m-%%d') as date_listed, rejected, rejected_by FROM tb_shortlist_history WHERE userid =  %s ORDER BY date_listed DESC", (personal["userid"]))
        shortlists = dictfetchall(c)
        personal["shortlists"] = shortlists
        
        c.execute("SELECT * FROM education WHERE userid = %s", (personal["userid"]))
        education = dictfetchall(c)
        
        if len(education) > 0:
            education = education[0]
            personal["education"] = education
        
        c.execute("SELECT history AS note FROM applicant_history WHERE actions = 'NOTES' AND userid =  %s", (personal["userid"]))
        notes = dictfetchall(c)
        personal["notes"] = notes
        
        if settings.DEBUG:
            client = MongoClient()
            db = client.test
        else:
            client = MongoClient(host=settings.MONGO_PROD, port=27017)
            db = client.prod
        
        applicants_collection = db.applicants
        
        
        applicants_collection.update({'userid':personal["userid"]}, {"$set":personal}, upsert=True)
        

@task(ignore_result=True)
def check_job_order_sync():
    if settings.DEBUG:
        db = MySQLdb.connect(**settings.DB_ARGS)
    else:
        db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        
    c = db.cursor()
    
    db_update = MySQLdb.connect(**settings.DB_ARGS)
    cursor = db_update.cursor()
    
    i = 1
    rows = 1000
    if settings.DEBUG:
        client = MongoClient()
        db = client.test
    else:
        client = MongoClient(host=settings.MONGO_PROD, port=27017)
        db = client.prod
    
    job_order_collection = db.job_orders
        
    while(True):
        cursor.execute("set autocommit = 1") 
        start = (i-1)*rows
        end = start + rows
        c.execute("SELECT tracking_code FROM mongo_job_orders LIMIT "+str(start)+", "+str(end))
        job_orders = dictfetchall(c)
        if len(job_orders) == 0:
            break
        
        for job_order in job_orders:
            mongo_cursor = job_order_collection.find_one({tracking_code:job_order["tracking_code"]})
            if mongo_cursor==None:
                cursor.execute("DELETE FROM mongo_job_orders WHERE tracking_code='"+str(job_order["tracking_code"])+"'")
        i += 1

@task(ignore_result=True)
def personal_sync(userid):
    logging.info("Celery received query for personal sync  %s" % userid)
    if userid == "all":
        if settings.DEBUG:
            db = MySQLdb.connect(**settings.DB_ARGS)
        else:
            db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
            
        c = db.cursor()
        i = 1
        rows = 1000
        while(True):
            start = (i-1)*rows
            end = start + rows
            c.execute("SELECT userid FROM personal LIMIT "+str(start)+", "+str(end))
            personals = dictfetchall(c)
            if len(personals) == 0:
                break
            for personal in personals:
                sync_personal_to_mongo(personal["userid"])
            i += 1
            
    else:
        sync_personal_to_mongo(userid)

@task(ignore_result=True)
def recruitment_sync(query):
    
    from CategorizedDetails import CategorizedDetails
    
    logging.info("Celery received query for recruitment sync from sc.remotestaff.com.au  %s" % query)
    
    if settings.DEBUG:
        db = MySQLdb.connect(**settings.DB_ARGS)
    else:
        db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
    
    c = db.cursor()
    c.execute(query)
    recruitment = dictfetchall(c)
    today = datetime.datetime.today()
    db_update = MySQLdb.connect(**settings.DB_ARGS)
    cursor = db_update.cursor()
    fmt = "%Y-%m-%d"
    
    for recruitment_item in recruitment:
        if recruitment_item["table"] == "job_sub_category_applicants":
            c.execute("SELECT sub_category_id, category_id, ratings, under_consideration, DATE_FORMAT(sub_category_applicants_date_created, '%%Y-%%m-%%d') AS sub_category_applicants_date_created FROM job_sub_category_applicants WHERE id = %s", [recruitment_item["key"]])
            jsca = dictfetchall(c)
            if len(jsca) > 0:
                jsca = jsca[0]
                recruitment_item["sub_category_id"] = jsca["sub_category_id"]
                recruitment_item["category_id"] = jsca["category_id"]
                recruitment_item["ratings"] = jsca["ratings"]
                recruitment_item["date_added"] = jsca["sub_category_applicants_date_created"]
                conv_date = datetime.datetime.strptime(recruitment_item["date_added"], "%Y-%m-%d")
                recruitment_item["date_added"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
                recruitment_item["under_consideration"] = jsca["under_consideration"]
                
            
            if recruitment_item["userid"]:
                c.execute("SELECT userid, lname, fname, middle_name, nick_name, gender, email, DATE_FORMAT(datecreated, '%%Y-%%m-%%d') AS datecreated , DATE_FORMAT(dateupdated, '%%Y-%%m-%%d') AS dateupdated, city, state, marital_status FROM personal WHERE userid = %s", [recruitment_item["userid"]])
                personal = dictfetchall(c)
                if len(personal) > 0:
                    personal = personal[0]
                    if personal["datecreated"]!=None:
                        conv_date = datetime.datetime.strptime(personal["datecreated"], "%Y-%m-%d")
                        personal["datecreated"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
                    if personal["dateupdated"]!=None:
                        conv_date_updated = datetime.datetime.strptime(personal["dateupdated"], "%Y-%m-%d")
                        personal["dateupdated"] = datetime.datetime(conv_date_updated.year, conv_date_updated.month, conv_date_updated.day, 0, 0)
                    recruitment_item["personal"] = personal
                else:
                    recruitment_item["personal"] = None
                
                
                c.execute("SELECT time_zone, s.time, s.type FROM staff_selected_timezones AS s WHERE userid = %s", [recruitment_item["userid"]])
                selected_timezones = dictfetchall(c)
                
                staff_selected_timezones = []
                for selected_timezone in selected_timezones:
                    td = selected_timezone["time"]
                    selected_timezone["time"] = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
                    staff_selected_timezones.append(selected_timezone)
                
                recruitment_item["staff_selected_timezones"] = staff_selected_timezones
                c.execute("SELECT skill, experience, proficiency FROM skills WHERE userid = %s", [recruitment_item["userid"]])
                skills = dictfetchall(c)
                recruitment_item["skills"] = skills
                c.execute("SELECT i.type AS inactive_type, DATE_FORMAT(date, '%%Y-%%m-%%d') AS date_inactive FROM inactive_staff AS i WHERE userid = %s", [recruitment_item["userid"]])
                inactive = dictfetchall(c)
                
                inactives = []
                for inactive_item in inactive:
                    if inactive_item["date_inactive"]!=None:
                        conv_date = datetime.datetime.strptime(inactive_item["date_inactive"], "%Y-%m-%d")
                        inactive_item["date_inactive"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
                        inactives.append(inactive_item)
                    
                recruitment_item["inactive"] = inactives
                c.execute("SELECT comments FROM evaluation_comments WHERE userid = %s",[recruitment_item["userid"]])
                evaluation_comments = dictfetchall(c)
                recruitment_item["evaluation_comments"] = evaluation_comments
                
                c.execute("SELECT available_status, available_notice, DATE_FORMAT(STR_TO_DATE(CONCAT(cj.amonth, ' ', cj.aday, ', ', cj.ayear), '%%M %%d, %%Y'), '%%Y-%%m-%%d') AS available_month, cj.latest_job_title FROM currentjob AS cj WHERE userid = %s", [recruitment_item["userid"]])
                currentjob = dictfetchall(c)
                if len(currentjob) > 0:
                    currentjob = currentjob[0]
                    try:
                        if currentjob["available_month"]!=None:
                            conv_date = datetime.datetime.strptime(currentjob["available_month"], "%Y-%m-%d")
                            currentjob["available_month"] = datetime.datetime(conv_date.year, conv_date.month, conv_date.day, 0, 0)
                    except:
                        currentjob["available_month"] = None 
                    recruitment_item["currentjob"] = currentjob
                else:
                    recruitment_item["currentjob"] = None
                
                c.execute("SELECT work_parttime, work_fulltime, work_freelancer, expected_minimum_salary, fulltime_sched, parttime_sched FROM evaluation WHERE userid = %s", [recruitment_item["userid"]])
                evaluation = dictfetchall(c)
                if len(evaluation) > 0:
                    evaluation = evaluation[0]
                    recruitment_item["evaluation"] = evaluation
                else:
                    recruitment_item["evaluation"] = None
                
                c.execute("SELECT rs.admin_id, a.admin_fname, a.admin_lname FROM recruiter_staff AS rs LEFT JOIN admin AS a ON a.admin_id = rs.admin_id WHERE rs.userid = %s", [recruitment_item["userid"]])
                assigned_recruiter = dictfetchall(c)
                if len(assigned_recruiter) > 0:
                    assigned_recruiter = assigned_recruiter[0]
                    recruitment_item["assigned_recruiter"] = assigned_recruiter
                else:
                    recruitment_item["assigned_recruiter"] = None
                    
                    
                c.execute("SELECT sc.leads_id as lead_id,l.fname AS lead_fname,l.lname AS lead_lname,sc.status,DATE_FORMAT(starting_date, '%%Y-%%m-%%d') as cdate, DATE_FORMAT(sc.resignation_date, '%%Y-%%m-%%d') as resig_date, DATE_FORMAT(sc.date_terminated, '%%Y-%%m-%%d') as term_date FROM subcontractors as sc LEFT JOIN leads as l on sc.leads_id= l.id WHERE userid =  %s ORDER BY starting_date DESC", [recruitment_item["userid"]])
                contracts = dictfetchall(c)
                recruitment_item["rs_employment_history"] = contracts
                
                c.execute("SELECT userid AS userid, client_name AS leads_id, position AS posting_id, job_category, id FROM tb_endorsement_history WHERE userid =  %s ORDER BY date_endoesed DESC", [recruitment_item["userid"]])
                endorsements = dictfetchall(c)
                recruitment_item["endorsements"] = endorsements
                
                c.execute("SELECT userid AS userid, position AS posting_id, status AS status, DATE_FORMAT(date_listed, '%%Y-%%m-%%d') as date_listed, rejected, rejected_by FROM tb_shortlist_history WHERE userid =  %s ORDER BY date_listed DESC", [recruitment_item["userid"]])
                shortlists = dictfetchall(c)
                recruitment_item["shortlists"] = shortlists
                
                
                c.execute("SELECT history AS note FROM applicant_history WHERE actions = 'NOTES' AND userid =  %s", [recruitment_item["userid"]])
                notes = dictfetchall(c)
                recruitment_item["notes"] = notes
                
                recruitment_item["monthly_full_time_rates"] = {}
                recruitment_item["hourly_full_time_rates"] = {}
                recruitment_item["monthly_part_time_rates"] = {}
                recruitment_item["hourly_part_time_rates"] = {}
                
                
                recruitment_item["monthly_full_time_rates"]["au"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 3, "monthly")
                recruitment_item["monthly_part_time_rates"]["au"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 3, "monthly")
                recruitment_item["hourly_part_time_rates"]["au"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 3, "hourly")
                recruitment_item["hourly_full_time_rates"]["au"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 3, "hourly")
                recruitment_item["monthly_full_time_rates"]["uk"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 4, "monthly")
                recruitment_item["monthly_part_time_rates"]["uk"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 4, "monthly")
                recruitment_item["hourly_part_time_rates"]["uk"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 4, "hourly")
                recruitment_item["hourly_full_time_rates"]["uk"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 4, "hourly")
                recruitment_item["monthly_full_time_rates"]["us"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 5, "monthly")
                recruitment_item["monthly_part_time_rates"]["us"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 5, "monthly")
                recruitment_item["hourly_part_time_rates"]["us"] = CategorizedDetails.getPartTimeRates(recruitment_item["userid"], 5, "hourly")
                recruitment_item["hourly_full_time_rates"]["us"] = CategorizedDetails.getFullTimeRates(recruitment_item["userid"], 5, "hourly")

                
                
        cursor.execute("set autocommit = 1") 
        if settings.DEBUG:
            client = MongoClient()
            db = client.test
        else:
            client = MongoClient(host=settings.MONGO_PROD, port=27017)
            db = client.prod
        recruitment_collection = db.recruitment
        if recruitment_item["date"] and recruitment_item["date"]!=None:
            date_created = datetime.datetime.strptime(recruitment_item["date"], "%Y-%m-%d")
            recruitment_item["date"] = datetime.datetime(date_created.year, date_created.month, date_created.day, 0, 0)
           
        mongo_cursor = recruitment_collection.find_one({'tracking_code':recruitment_item["tracking_code"]})
        try:
            if mongo_cursor!=None:
                recruitment_collection.update({'tracking_code':recruitment_item["tracking_code"]}, {"$set":recruitment_item})
            else:
                recruitment_collection.insert(recruitment_item)
        except:
            recruitment_collection.update({'tracking_code':recruitment_item["tracking_code"]}, {"$set":recruitment_item})
        try:
            cursor.execute("INSERT INTO mongo_recruitment_entries(tracking_code,userid,date_created) VALUES (%s, %s, %s)", (recruitment_item["tracking_code"], recruitment_item["userid"], today))
        except:
            pass
    cursor.close()
    db_update.commit()
        
        
@task(ignore_result=True)
def job_order_sync(query):
    logging.info("Receive job order sync for query %s on sc.remotestaff.com.au" % query)
    #logging.info("Celery received query for job order sync  %s" % query)
    
    if settings.DEBUG:
        db = MySQLdb.connect(**settings.DB_ARGS)
    else:
        db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        
    c = db.cursor()
    c.execute(query)
    job_orders = dictfetchall(c)
    lister = StaffLister()
    fmt = "%Y-%m-%d"
    db_update = MySQLdb.connect(**settings.DB_ARGS)
    cursor = db_update.cursor()
    for job_order in job_orders:
        logging.info("Syncing "+job_order["tracking_code"])
        try:
            job_order["posting_id"]
        except:
            job_order["posting_id"] = None
            
        if job_order["service_type"] == "REPLACEMENT" and job_order["created_reason"] == "Closed-To-Replacement":
            trace_id = job_order["gs_job_titles_details_id"]
            while(True):
                c.execute("SELECT link_order_id FROM gs_job_titles_details WHERE gs_job_titles_details_id=%s", [trace_id])
                linked_order_id = dictfetchall(c)
                if len(linked_order_id)>0:
                    if linked_order_id[0]["link_order_id"] and linked_order_id[0]["link_order_id"]!=None:
                        trace_id = linked_order_id[0]["link_order_id"]
                        c.execute("SELECT id AS posting_id FROM posting WHERE job_order_id=%s", [trace_id])
                        posting_id = dictfetchall(c)
                        if len(posting_id)>0:
                            job_order["posting_id"] = posting_id[0]["posting_id"]
                            break
                    else:
                        break
                else:
                    break
        
        if job_order["merge_status"] == "MERGE":
            job_order["endorsed"] = lister.getMergedStaff(job_order["merged_order_id"], lister.ENDORSED)
            job_order["interviewed"] = lister.getMergedStaff(job_order["merged_order_id"], lister.INTERVIEWED)
            job_order["hired"] = lister.getMergedStaff(job_order["merged_order_id"], lister.HIRED)
            job_order["cancelled"] = lister.getMergedStaff(job_order["merged_order_id"], lister.CANCELLED)
            job_order["rejected"] = lister.getMergedStaff(job_order["merged_order_id"], lister.REJECTED)
            job_order["ontrial"] = lister.getMergedStaff(job_order["merged_order_id"], lister.ONTRIAL)
            job_order["shortlisted"] = lister.getMergedStaff(job_order["merged_order_id"], lister.SHORTLISTED)
        else:
            if job_order["service_type"] == "REPLACEMENT" and job_order["created_reason"] == "Closed-To-Replacement":
                job_order["endorsed"] = lister.getEndorsedStaff(job_order["leads_id"], job_order["posting_id"])
                job_order["interviewed"] = lister.getInterviewedStaff(job_order["leads_id"], job_order["posting_id"], None, None, None, job_order["tracking_code"])
                job_order["hired"] = lister.getHiredStaff(job_order["leads_id"], job_order["posting_id"])
                job_order["cancelled"] = lister.getCancelledStaff(job_order["leads_id"], job_order["posting_id"])
                job_order["rejected"] = lister.getRejectedStaff(job_order["leads_id"], job_order["posting_id"])
                job_order["ontrial"] = lister.getOnTrialStaff(job_order["leads_id"], job_order["posting_id"])
                job_order["shortlisted"] = lister.getShortlistedStaff(job_order["posting_id"])
            else:
                job_order["endorsed"] = lister.getEndorsedStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"], job_order=job_order)
                job_order["interviewed"] = lister.getInterviewedStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"], job_order["tracking_code"])
                job_order["hired"] = lister.getHiredStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"])
                job_order["cancelled"] = lister.getCancelledStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"])
                job_order["rejected"] = lister.getRejectedStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"])
                job_order["ontrial"] = lister.getOnTrialStaff(job_order["leads_id"], job_order["posting_id"], job_order["gs_job_role_selection_id"], job_order["jsca_id"], job_order["date_filled_up"])
                job_order["shortlisted"] = lister.getShortlistedStaff(job_order["posting_id"])
        
        if job_order["date_filled_up"] and job_order["date_filled_up"]!=None:
            date_filled_up = datetime.datetime.strptime(job_order["date_filled_up"], "%Y-%m-%d")
            job_order["date_filled_up"] = datetime.datetime(date_filled_up.year, date_filled_up.month, date_filled_up.day, 0, 0)
            
            
        if job_order["date_closed"] and job_order["date_closed"]!=None:
            date_closed = datetime.datetime.strptime(job_order["date_closed"], "%Y-%m-%d")
            job_order["date_closed"] = datetime.datetime(date_closed.year, date_closed.month, date_closed.day, 0, 0)
       
        if job_order["timestamp"] and job_order["timestamp"]!=None:
            timestamp = datetime.datetime.strptime(job_order["timestamp"], "%Y-%m-%d %H:%M:%S")
            job_order["timestamp"] = datetime.datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute)
            
        job_order["order_status"] = getStatus(job_order["status"])
        job_order["recruiters"] = getRecruiters(job_order)
        job_order["last_contact"] = getLastContact(job_order)
        
        c.execute("SELECT * FROM mongo_job_orders_multi_statuses WHERE tracking_code=%s ORDER BY date_created DESC LIMIT 1", [job_order["tracking_code"]])
        multi_status = dictfetchall(c)
        if len(multi_status) > 0:
            multi_status = multi_status[0]
            if multi_status["assigning_status"]:
                job_order["sub_order_status"] = multi_status["assigning_status"]
            elif multi_status["hiring_status"]:
                job_order["sub_order_status"] = multi_status["hiring_status"]
            elif multi_status["decision_status"]:
                job_order["sub_order_status"] = multi_status["decision_status"]
             
            job_order["status_last_update"] = multi_status["date_updated"]
            
        else:
            if job_order["order_status"] == "Open":
                job_order["sub_order_status"] = "OPEN"
            elif job_order["order_status"] == "Closed":
                job_order["sub_order_status"] = "CLOSED_HIRED"
            elif job_order["order_status"] == "On Trial":
                job_order["sub_order_status"] = "CLOSED_TRIAL"
            elif job_order["order_status"] == "Did not push through":
                job_order["sub_order_status"] = "CLOSED_DID_NOT_PUSH_THROUGH"
            elif job_order["order_status"] == "On Hold":
                job_order["sub_order_status"] = "CLOSED_HOLD"
            else:
                job_order["sub_order_status"] = "OPEN"
                
    
        client = MongoClient(host=settings.MONGO_PROD, port=27017)
        db = client.prod
        job_order_collection = db.job_orders
        
        mongo_cursor = job_order_collection.find_one({'tracking_code':job_order["tracking_code"]})
        today = datetime.datetime.today()
        next_sync_date = today + datetime.timedelta(seconds=360)
        today = datetime.datetime.strftime(today, "%Y-%m-%d %H:%M:%S")
        next_sync_date = datetime.datetime.strftime(next_sync_date, "%Y-%m-%d %H:%M:%S")
        try:
            if mongo_cursor!=None:
                job_order_collection.update({'tracking_code':job_order["tracking_code"]}, {"$set":job_order})    
            else:
                job_order_collection.insert(job_order)
        except:
            pass
        c.execute("SELECT tracking_code FROM mongo_job_orders WHERE tracking_code=%s", [job_order["tracking_code"]])
        tracking = dictfetchall(c)
        if len(tracking) == 0:
            if job_order["date_filled_up"]!=None:
                date_ordered = datetime.datetime.strftime(job_order["date_filled_up"], "%Y-%m-%d")
            else:
                date_ordered = '1970-01-01'
            if job_order["date_closed"]!=None:
                date_closed = datetime.datetime.strftime(job_order["date_closed"], "%Y-%m-%d")
            else:
                date_closed = '1970-01-01'
            cursor.execute("set autocommit = 1") 
            cursor.execute("INSERT INTO mongo_job_orders(tracking_code, leads_id, next_sync_date_time, date_created, job_title, date_ordered, date_closed) VALUES(%s, %s, %s, %s, %s, %s, %s)", [job_order["tracking_code"],job_order["leads_id"], next_sync_date, today, job_order["job_title"], date_ordered, date_closed])
        
        try:
            #try one
            #todo invoke
            #call API for searching
            import urllib
            import urllib2
            
            import json
            
            
            url = settings.API_URL+"/job-order-reporting/ping-job-order-finish-update/?tracking_code="+job_order["tracking_code"]
            
            #api_url = settings.API_URL+"/solr-index/sync-candidates/"
            urllib2.urlopen(url)
        except:
            try:
                 #try two
                #todo invoke
                #call API for searching
                import urllib
                import urllib2
                
                import json
                
                
                url = settings.API_URL+"/job-order-reporting/ping-job-order-finish-update/?tracking_code="+job_order["tracking_code"]
                
                #api_url = settings.API_URL+"/solr-index/sync-candidates/"
                urllib2.urlopen(url)
            except:
                pass
        
        
        
    cursor.close()
    db_update.commit()
    
    try:
        #todo invoke
        #call API for searching
        import urllib
        import urllib2
        import json
        api_url = settings.API_URL+"/solr-index/sync-candidates/"
        urllib2.urlopen(api_url)
    except:
        pass
        
            