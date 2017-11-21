#!/usr/bin/env python
#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2013-03-06  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added skype notif before execution
#   2013-03-04  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added task for scheduling staffs salary update

from celery.task import task
from celery.execute import send_task
from subprocess import call
import settings
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

@task(ignore_result=True)
def ScheduleActivation(subcontractors_id):
    """This will call a php cli
    """
    logging.info('calling %s %s' % (settings.PHP_SCRIPT, subcontractors_id))
    call(["php", settings.PHP_SCRIPT, "%s" % subcontractors_id])


@task(ignore_result=True)
def StaffSalaryUpdate(subcon_id):
    """This will call a php cli
    """
    logging.info('Executing %s %s' % (settings.PHP_UPDATE_STAFF_SALARY, subcon_id))
    send_task('skype_messaging.notify_devs', ['Executing %s %s' % (settings.PHP_UPDATE_STAFF_SALARY, subcon_id)])
    call(["php", settings.PHP_UPDATE_STAFF_SALARY, "%s" % subcon_id])
    send_task('skype_messaging.notify_devs', ['Executed %s %s' % (settings.PHP_UPDATE_STAFF_SALARY, subcon_id)])


def run_tests():
    """
    >>> StaffSalaryUpdate(1234)
    """

if __name__ == "__main__":
    import doctest
    doctest.testmod()

