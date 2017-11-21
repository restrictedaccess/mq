#   2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
#   2012-11-20 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   task for compacting couchdb database

import settings
import couchdb
from celery.task import task

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import re


@task(ignore_result=True)
def compact(database):
    """compacts couchdb and its views
    """
    logging.info('compacting couchdb : %s' % database)
    s = couchdb.Server(settings.COUCH_DSN)
    db = s[database]
    db.compact()

    for r in db.view('_all_docs', startkey='_design/', endkey='_design0'):
        design_view = re.sub('_design/', '', r['id'])
        logging.info('compacting couchdb view : %s ' % design_view)
        db.compact(design_view)


if __name__ == '__main__':
    compact('rssc')
