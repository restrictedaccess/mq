#   2013-04-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   ignored result and added time_limit
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-02-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   explicitly close mysql connection pool
#   2013-01-29 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used persistent connection
#   2012-11-26 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   initial hack

import settings
from pprint import pprint, pformat

from celery.task import task, Task
from celery.execute import send_task


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

from sqlalchemy.sql import text
from persistent_mysql_connection import engine

SECONDS_SLOW_QUERY = 10
SECONDS_KILL_QUERY = 60


@task(ignore_result=True, time_limit=20)
def run():
    """
    return a list of items suitable for invoicing
    """

    #get timesheets
    sql = text("""
        SHOW FULL PROCESSLIST
        """)

    conn = engine.connect()
    data = conn.execute(sql).fetchall()

    for d in data:
        if d['Command'] != 'Query':
            continue

        if d['Time'] >= SECONDS_KILL_QUERY:
            report = """Sorry guys, I need to kill this query.\n\n%s
            """ % (pformat(d.items(), 4))
            send_task("notify_devs.send", ("KILL QUERY REPORT", report))
            sql = text("""
                KILL :process_id
                """)
            conn.execute(sql, process_id=d['Id'])
            continue

        if d['Time'] > SECONDS_SLOW_QUERY:
            report = """Please optimize this query.\n\n%s
            """ % (pformat(d.items(), 4))
            send_task("notify_devs.send", ("SLOW QUERY REPORT", report))

    conn.close()
    

if __name__ == '__main__':
    run()
