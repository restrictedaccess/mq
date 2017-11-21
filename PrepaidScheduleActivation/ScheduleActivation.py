#   2012-04-09  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   fixed DST issues
#   2012-04-05  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   moved the task to celery directory

import settings
import MySQLdb
from datetime import datetime, time, timedelta
from pytz import timezone
import re

import smtplib
from email.mime.text import MIMEText

class SubcontractorsTemp:
    def __send_email_alert__(self, subject, msg):
        msg = MIMEText(msg)
        if settings.DEBUG:
            msg['Subject'] = 'TEST %s' % subject
        else:
            msg['Subject'] = subject

        msg['From'] = 'remotestaff prepaid activation rabbitmq<noreply@remotestaff.com.au>'
        msg['To'] = settings.EMAIL_ALERT

        s = smtplib.SMTP(host= settings.SMTP_CONFIG['server'],
            port = settings.SMTP_CONFIG['port'])
        s.login(settings.SMTP_CONFIG['username'],
            settings.SMTP_CONFIG['password']
            )
        s.sendmail('noreply@remotestaff.com.au',
            settings.EMAIL_ALERT, msg.as_string())
        s.quit()

    def get_execution_date(self, subcontractors_temp_id):
        db = MySQLdb.connect(**settings.DB_ARGS)
        c = db.cursor()
        c.execute("SELECT id, subcontractors_id FROM subcontractors_temp where prepaid = 'yes' and temp_status = 'new' and id = %s", (subcontractors_temp_id))
        result = c.fetchone()
        if result == None:
            self.__send_email_alert__('Failed Scheduled Activation', 'You sent an id of %s' % subcontractors_temp_id)
            return None

        subcontractors_id = result[1]
        if subcontractors_id == None:
            table = 'subcontractors_temp'
            sid = subcontractors_temp_id
        else:
            table = 'subcontractors'
            sid = subcontractors_id

        sql = "SELECT prepaid_start_date, client_timezone, client_start_work_hour from %s where id = %s" % (table, sid)
        c.execute(sql)

        result = c.fetchone()
        if result == None:
            self.__send_email_alert__('Failed Scheduled Activation', 'No records found for query %s' % sql)
            return None

        prepaid_start_date, client_timezone, client_start_work_hour = result

        if prepaid_start_date == None:
            self.__send_email_alert__('Failed Scheduled Activation', 'prepaid_start_date is NULL for query %s' % sql)
            return None

        if client_timezone == None:
            self.__send_email_alert__('Failed Scheduled Activation', 'client_timezone is NULL for query %s' % sql)
            return None

        utc_tz = timezone('UTC')
        client_timezone = client_timezone.encode('ascii')
        client_tz = timezone(client_timezone)
        ph_tz = timezone('Asia/Manila')

        if client_start_work_hour == None or client_start_work_hour == '':
            #no client_start_work_hour found, just return at midnight Asia/Manila time
            time_start_work = time(0, 0)
            client_time = datetime(prepaid_start_date.year, prepaid_start_date.month, prepaid_start_date.day, tzinfo=client_tz)
            eta = client_time.astimezone(ph_tz)
            eta = eta.replace(hour=0, minute=0)
        else:
            x = re.split(':', client_start_work_hour)
            if len(x) == 1:
                time_start_work = time(int(x[0]), 0)
            else:
                time_start_work = time(int(x[0]), int(x[1]))

            client_time = datetime(prepaid_start_date.year, 
                prepaid_start_date.month, 
                prepaid_start_date.day, 
                time_start_work.hour,
                time_start_work.minute)
            client_time = client_tz.localize(client_time)
            eta = client_time.astimezone(ph_tz)
            eta = eta - timedelta(hours=settings.HOURS_BEFORE_CONVERSION)

        c.close()
        db.close()
        return eta


if __name__ == '__main__':
    s = SubcontractorsTemp()
    print s.get_execution_date(872)
    print s.get_execution_date(876)
