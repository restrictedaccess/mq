import settings
import couchdb
import re

s = couchdb.Server(settings.COUCH_DSN)
db = s['resume']
data = db.view('_all_docs', include_docs=True)

for d in data:
    doc = d.doc
    update_skill_ids = []
    if doc.has_key('skills') != False:
        for k, v in doc['skills'].iteritems():
            skill = v['skill']
            if re.search('reserach', skill, re.I) != None:
                update_skill_ids.append(k)

    if len(update_skill_ids) > 0:
        skills = doc['skills']
        for skill_id in update_skill_ids:
            skills[skill_id]['skill'] = 'Internet Research'

        doc['skills'] = skills
        db.save(doc)


