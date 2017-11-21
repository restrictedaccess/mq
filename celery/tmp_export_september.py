#! /usr/bin/env python

import settings
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from pprint import pprint
import csv

engine = create_engine(settings.MYSQL_DSN)
conn = engine.connect()

sql = text("""
    SELECT t.id, t.userid, p.fname as staff_fname, p.lname as staff_lname, 
    t.leads_id, l.fname as leads_fname, l.lname as leads_lname, t.subcontractors_id
    FROM timesheet as t
    JOIN personal as p on t.userid=p.userid
    JOIN leads as l on t.leads_id=l.id
    WHERE t.month_year='2012-09-01 00:00:00'
    AND t.status='open'
""")

timesheets = conn.execute(sql).fetchall()

data = {}
for t in timesheets:
    k = '%s %s' % (t.userid, t.leads_id)
    x = [t.id, t.userid, t.staff_fname, t.staff_lname, t.leads_id, t.leads_fname, t.leads_lname, t.subcontractors_id]
    if data.has_key(k) == False:
        data[k] = [x]
    else:
        data[k].append(x)

multiple_records = []
for k, v in data.iteritems():
    if len(v) >= 2:
        for x in v:
            multiple_records.append(x)


f = csv.writer(open('multiple_timesheets.csv', 'wb'))
f.writerow(['Timesheet ID', 'Staff ID', 'Staff Firstname', 'Staff Lastname', 'Client ID', 'Client Firstname', 'Client Lastname', 'Subcontract ID'])
f.writerows(multiple_records)
