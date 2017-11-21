#!/usr/bin/env python
#   2014-02-20  Allanaire Tapion <allan.t@remotestaff.com.au>
#   -   responsible for getting categorized related information

import settings
import logging
import MySQLdb
import json
from email.mime.text import MIMEText
import sys
from celery.execute import send_task

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

class CategorizedDetails:
    
    @staticmethod
    def getPartTimeRates(userid, currency_id, adv_rates):
        
        sql = "SELECT p.amount FROM staff_rate s, product_price_history p WHERE p.currency_id=%s AND s.userid=%s AND s.part_time_product_id=p.product_id ORDER BY p.date DESC LIMIT 1"
        if settings.DEBUG:
            db = MySQLdb.connect(**settings.DB_ARGS)
        else:
            db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        c = db.cursor()
        c.execute(sql, [currency_id, userid])
        per_month = dictfetchall(c)
            
        if len(per_month) > 0:
            per_month = per_month[0]["amount"]
            
            yearly = per_month * 12
            weekly = yearly / 52
            daily = weekly / 5
            per_hour = daily / 4
            
            if adv_rates == "monthly":
                return float(per_month)
            else:
                return float(per_hour)
            
        else:
            return 0
        
    
    @staticmethod
    def getFullTimeRates(userid, currency_id, adv_rates):
        
        sql = "SELECT p.amount FROM staff_rate s, product_price_history p WHERE p.currency_id=%s AND s.userid=%s AND s.product_id=p.product_id ORDER BY p.date DESC LIMIT 1"
        if settings.DEBUG:
            db = MySQLdb.connect(**settings.DB_ARGS)
        else:
            db = MySQLdb.connect(**settings.DB_ARGS_SELECT_ONLY)
        c = db.cursor()
        c.execute(sql, [currency_id, userid])
        per_month = dictfetchall(c)
        if len(per_month) > 0:
            per_month = per_month[0]["amount"]
            
            yearly = per_month * 12
            weekly = yearly / 52
            daily = weekly / 5
            per_hour = daily / 8
            
            if adv_rates == "monthly":
                return float(per_month)
            else:
                return float(per_hour)
            
        else:
            return 0
        