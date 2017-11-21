"""
initial check showed 585, 843, 1698, 1731, 1857 has errors
"""

import settings
import couchdb
import re
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from datetime import datetime
import pytz
from pytz import timezone

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')


def GetPhTimeStr():                                                     
    """returns a philippines time string in 2010-08-31 00:12:45 format
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    return now.astimezone(phtz).strftime('%Y-%m-%d %H:%M:%S')


def main():
    #get mysql records
    sql = text("""SELECT id from subcontractors where status='ACTIVE'""")
    engine = create_engine(settings.MYSQL_DSN)
    conn = engine.connect()
    subcon_ids = conn.execute(sql).fetchall()
    subcon_ids = map(lambda x:int(x[0]), subcon_ids)

    #get records from couchdb
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['rssc']
    r = db.view('subcontractor/active')

    #loop and compare
    for d in r:
        sid = re.sub('subcon-', '', d['id'])
        sid = int(sid)
        if sid not in subcon_ids:
            logging.info('%s :: %s not in sync with rssc' % (__file__, sid))
            doc = db.get('subcon-%s' % sid)
            if doc.has_key('history'):
                history = doc['history']
            else:
                history = []
            history.append(
                dict(
                    date = GetPhTimeStr(),
                    note = 'disabled by %s' % __file__
                    )
                )
            doc['history'] = history
            doc['active'] = 'N'
            db.save(doc)


if __name__ == '__main__':
    main()
