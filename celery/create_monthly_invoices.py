import couchdb
from celery.execute import send_task
WORKING_DAYS = 17

s = couchdb.Server('http://replication:r2d2rep@iweb10:5984')
db = s['client_docs']

for r in db.view('client/settings', include_docs=True):
    doc = r.doc
    if doc.has_key('days_before_suspension'):
        if doc['days_before_suspension'] == -30:
            client_id = doc['client_id']
            t = send_task('prepaid_create_invoice.create_invoice', [client_id, WORKING_DAYS])
            r = t.get()
            if type(r) is str:
                print r
            else:
                if r.has_key('order_id'):
                    print '%s doc_id=%s' % (r['order_id'], r['_id'])
                else:
                    print r
