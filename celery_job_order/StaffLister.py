#!/usr/bin/env python
#   2013-08-06  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -   responsible for getting the staff involved in the job order

import settings
import logging
import MySQLdb
import json
from email.mime.text import MIMEText
from pymongo import MongoClient
import sys
from celery.execute import send_task
import datetime
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

class StaffLister:
    ENDORSED = "endorsed"
    INTERVIEWED = "interviewed"
    REJECTED = "rejected"
    SHORTLISTED = "shortlisted"
    HIRED = "hired"
    ONTRIAL = "ontrial"
    CANCELLED = "cancelled"
    
    def getMoreDetailsForMerge(self, item):
        row = {}
        if item["service_type"]=="ASL":
            sql = "SELECT session_id FROM tb_request_for_interview AS tbr WHERE tbr.job_sub_category_applicants_id = '{jsca_id}' AND DATE(date_added) = DATE('{date_added}')".format(jsca_id=item["jsca_id"], date_added=item["date_added"])
            db = MySQLdb.connect(**settings.DB_ARGS)
            c = db.cursor()
            c.execute(sql)
            session_id = c.fetchone()
            if session_id:
                sql = "SELECT gs_jtd.gs_job_role_selection_id AS gs_job_role_selection_id, p.id AS posting_id FROM gs_job_titles_details AS gs_jtd LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id LEFT JOIN posting AS p ON p.job_order_id = gs_jtd.gs_job_titles_details_id WHERE DATE(gs_jrs.request_date_added) = DATE('{date_added}') AND gs_jrs.jsca_id = '{jsca_id}' AND gs_jrs.leads_id = '{leads_id}'".format(date_added=item["date_added"], jsca_id=item["jsca_id"], leads_id=item["lead_id"])
                c.execute(sql)
                rows = dictfetchall(c)
                if len(rows)>0:
                    row = rows[0]
            row["date_filled_up"] = item["date_added"]
            row["jsca_id"] = item["jsca_id"]
            row["leads_id"] = item["lead_id"]
        else:
            if (item["gs_job_title_details_id"]!=None):
                sql = "SELECT gs_jtd.gs_job_role_selection_id AS gs_job_role_selection_id, CASE WHEN gs_jrs.session_id IS NOT NULL THEN DATE_FORMAT(gs_jrs.request_date_added, '%Y-%m-%d') ELSE DATE_FORMAT(gs_jtd.date_filled_up, '%Y-%m-%d') END AS date_filled_up, gs_jrs.leads_id AS leads_id, gs_jrs.jsca_id AS jsca_id, p.id AS posting_id FROM gs_job_titles_details AS gs_jtd LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id LEFT JOIN posting AS p ON p.job_order_id = gs_jtd.gs_job_titles_details_id WHERE gs_jtd.gs_job_titles_details_id = {gs_job_titles_details_id}".format(gs_job_titles_details_id=item["gs_job_title_details_id"])
                db = MySQLdb.connect(**settings.DB_ARGS)
                c = db.cursor()
                c.execute(sql)
                rows = dictfetchall(c)
                if len(rows)>0:
                    row = rows[0]
        return row
        
    def getMergedStaff(self, merged_order_id, type):
        sql = "SELECT * FROM merged_order_items WHERE merged_order_id = '{merged_order_id}'".format(merged_order_id=merged_order_id)
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute(sql)
        merged_orders = dictfetchall(c)
        result = []
        for item in merged_orders:
            list = self.getMoreDetailsForMerge(item)
            foundStaff = []
            if list:
                if type == self.ENDORSED:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getEndorsedStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getEndorsedStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                elif type == self.INTERVIEWED:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getInterviewedStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getInterviewedStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                elif type == self.SHORTLISTED:
                    if item["service_type"] != "ASL":
                        foundStaff = self.getShortlistedStaff(list["posting_id"])
                elif type == self.HIRED:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getHiredStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getHiredStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                elif type == self.CANCELLED:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getCancelledStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getCancelledStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                elif type == self.REJECTED:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getRejectedStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getRejectedStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                elif type == self.ONTRIAL:
                    if item["service_type"] == "ASL":
                        foundStaff = self.getOnTrialStaff(list["leads_id"], None, None, list["jsca_id"], list["date_filled_up"])
                    else:
                        foundStaff = self.getOnTrialStaff(list["leads_id"], list["posting_id"], list["gs_job_role_selection_id"])
                    
                for result_item in foundStaff:
                    
                    found = False
                    for listed_item in result:
                        if listed_item["userid"] == result_item["userid"]:
                            found = True
                    
                    if not found:
                        result.append(result_item)
        
        return result
    
    
    
    def getEndorsedStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None, job_order=None):
        sqls = []
        staffs = []
        
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        if posting_id:
            
            
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT end.id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d'))  AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS end LEFT JOIN posting AS pos ON pos.id = end.position LEFT JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name LEFT JOIN personal AS pers ON pers.userid = end.userid LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.position = '{posting_id}' GROUP BY end.userid".format(posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None:
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.status = 'NEW' AND tbr.service_type = 'ASL' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id, tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.status = 'NEW' AND tbr.service_type = 'ASL' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        
        sql = " UNION ".join(sqls)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
        
        for i, staff in enumerate(staffs):
            if staff["date"]!=None:
                date = datetime.datetime.strptime(staff["date"], "%Y-%m-%d")
                staffs[i]["date_created"] = datetime.datetime(date.year, date.month, date.day, 0, 0)   
            
        return staffs
    
    def getShortlistedStaff(self, posting_id):
        sql = "SELECT sh.id,sh.userid,sh.rejected, CONCAT(DATE_FORMAT(sh.date_listed, '%Y-%m-%d')) AS date, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_shortlist_history AS sh LEFT JOIN personal AS pers ON pers.userid = sh.userid LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE sh.position = '{posting_id}' GROUP BY sh.id".format(posting_id=posting_id)
        
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute(sql)
        staffs = dictfetchall(c)
        
        for i, staff in enumerate(staffs):
            if staff["date"]!=None:
                date = datetime.datetime.strptime(staff["date"], "%Y-%m-%d")
                staffs[i]["date_created"] = datetime.datetime(date.year, date.month, date.day, 0, 0)
        
        return staffs

    def getInterviewedStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None, tracking_code=None):
        sqls = []
        staffs = []
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        old_order = True
        if tracking_code != None:
            sql = "SELECT tb_request_for_interview_id AS id, CONCAT('0') AS tbr_id, ic.candidate_id AS userid, CONCAT(DATE_FORMAT(tbr.date_interview, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, (CASE WHEN (tbr.status = 'REJECTED') THEN 1 ELSE 0 END) AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM interview_candidate AS ic INNER JOIN tb_request_for_interview AS tbr ON tbr.id = ic.tb_request_for_interview_id INNER JOIN personal AS pers ON pers.userid = ic.candidate_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE ic.tracking_code = '" + tracking_code + "' AND tbr.service_type = 'CUSTOM' AND tbr.status NOT IN ('NEW', 'CANCELLED')"
            c.execute(sql)
            interviewed_staff = dictfetchall(c)
            if len(interviewed_staff) > 0:
                old_order = False
                
                sqls.append(sql)
                
        if posting_id and old_order:
            
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT end.id AS id, tbr.id AS tbr_id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS end LEFT JOIN posting AS pos ON pos.id = end.position INNER JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name INNER JOIN personal AS pers ON pers.userid = end.userid LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.client_name = '{leads_id}' AND end.position = '{posting_id}' AND tbr.service_type = 'CUSTOM' AND tbr.status NOT IN ('NEW', 'CANCELLED') GROUP BY end.userid".format(leads_id=leads_id, posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None and old_order:
            sql = "SELECT tbr.id AS id,CONCAT('0') AS tbr_id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status NOT IN ('NEW', 'CANCELLED') GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id AS id,CONCAT('0') AS tbr_id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status NOT IN ('NEW', 'CANCELLED') GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
            if len(staffs) == 0:
                sql = "SELECT tbr.id AS id, tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status NOT IN ('NEW', 'CANCELLED') GROUP BY tbr.applicant_id".format(leads_id=leads_id, date_added=date_added)
                c.execute(sql)
                staffs = dictfetchall(c)
        #Injected By Josef Balisalisa START       
        if tracking_code != None:
            
            client = MongoClient(host=settings.MONGO_PROD, port=27017)
            db = client.prod
            candidates_job_order_collection = db.candidates_job_order
            
            found_job_orders = candidates_job_order_collection.find({"tracking_code":tracking_code, "recruitment_stage":"interviewed"})
            
            if found_job_orders.count() > 0:
                for candidate_job_order in found_job_orders:
                    found_staff = False;
                    for current_staff in staffs:
                        if int(current_staff["userid"]) == int(candidate_job_order["userid"]):
                            found_staff = True;
                    if not found_staff:
                        staffs.append({"userid":int(candidate_job_order["userid"]), "date":candidate_job_order["date"], "fullname":candidate_job_order["fullname"]})
        logging.info("candidate_job_order %s " % staffs)
        #Injected By Josef Balisalisa END
        
        return staffs
    
    def getHiredStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None):
        sqls = []
        staffs = []
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        if posting_id:
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT end.id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS end LEFT JOIN posting AS pos ON pos.id = end.position INNER JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name INNER JOIN personal AS pers ON pers.userid = end.userid  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.client_name = '{leads_id}' AND end.position = '{posting_id}' AND tbr.service_type = 'CUSTOM' AND tbr.status = 'HIRED' GROUP BY end.userid".format(leads_id=leads_id, posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None:
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'HIRED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'HIRED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
        return staffs
    
    def getRejectedStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None):
        sqls = []
        staffs = []
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        if posting_id:
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT tbr.id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS end LEFT JOIN posting AS pos ON pos.id = end.position INNER JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name INNER JOIN personal AS pers ON pers.userid = end.userid LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.client_name = '{leads_id}' AND end.position = '{posting_id}' AND tbr.service_type = 'CUSTOM' AND tbr.status = 'REJECTED' GROUP BY end.userid".format(leads_id=leads_id, posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None:
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'REJECTED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'REJECTED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
        return staffs
    
    def getCancelledStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None):
        sqls = []
        staffs = []
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        if posting_id:
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT end.id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS `end` LEFT JOIN posting AS pos ON pos.id = end.position INNER JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name INNER JOIN personal AS pers ON pers.userid = end.userid  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.client_name = '{leads_id}' AND end.position = '{posting_id}' AND tbr.service_type = 'CUSTOM' AND tbr.status = 'CANCELLED' GROUP BY end.userid".format(leads_id=leads_id, posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None:
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'CANCELLED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'CANCELLED' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
        return staffs
    
    def getOnTrialStaff(self, leads_id, posting_id, gs_job_role_selection_id=None, jsca_id=None, date_added=None):
        sqls = []
        staffs = []
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        if posting_id:
            #get more details for posting
            sql = "SELECT gs_jrs.leads_id, gs_jrs.jsca_id FROM posting AS p LEFT JOIN gs_job_titles_details AS gs_jtd ON gs_jtd.gs_job_titles_details_id = p.job_order_id LEFT JOIN gs_job_role_selection AS gs_jrs ON gs_jrs.gs_job_role_selection_id = gs_jtd.gs_job_role_selection_id WHERE p.id = "+str(posting_id)
            c.execute(sql)
            info = dictfetchall(c)
            if len(info) > 0:
                info = info[0]
                leads_id = info["leads_id"]
                jsca_id = info["jsca_id"]
            
            sql = "SELECT end.id,end.userid AS userid, CONCAT(DATE_FORMAT(end.date_endoesed, '%Y-%m-%d')) AS date, CONCAT('endorsement') AS type, end.rejected,  CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_endorsement_history AS end LEFT JOIN posting AS pos ON pos.id = end.position INNER JOIN tb_request_for_interview AS tbr ON tbr.applicant_id = end.userid AND tbr.leads_id = end.client_name INNER JOIN personal AS pers ON pers.userid = end.userid  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE end.client_name = '{leads_id}' AND end.position = '{posting_id}' AND tbr.service_type = 'CUSTOM' AND tbr.status = 'ON TRIAL' GROUP BY end.userid".format(leads_id=leads_id, posting_id=posting_id)
            sqls.append(sql)
        if leads_id and jsca_id and date_added and jsca_id != None and date_added!=None:
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id LEFT JOIN job_sub_category_applicants AS jsca ON jsca.id = tbr.job_sub_category_applicants_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND jsca.sub_category_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'ON TRIAL' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
            sql = "SELECT tbr.id,tbr.applicant_id AS userid, DATE_FORMAT(tbr.date_interview, '%Y-%m-%d') AS date, CONCAT('request') AS type, CONCAT('0') AS rejected, CONCAT(pers.fname, ' ', pers.lname) AS fullname, rs.admin_id AS assigned_recruiter FROM tb_request_for_interview AS tbr LEFT JOIN personal AS pers ON pers.userid = tbr.applicant_id  LEFT JOIN recruiter_staff AS rs ON rs.userid = pers.userid WHERE tbr.leads_id = '{leads_id}' AND tbr.job_sub_category_applicants_id='{jsca_id}' AND DATE(tbr.date_added) = DATE('{date_added}') AND tbr.service_type = 'ASL' AND tbr.status = 'ON TRIAL' GROUP BY tbr.applicant_id".format(leads_id=leads_id, jsca_id=jsca_id, date_added=date_added)
            sqls.append(sql)
        if len(sqls) > 0:
            sql = " UNION ".join(sqls)
            c.execute(sql)
            staffs = dictfetchall(c)
        return staffs
    