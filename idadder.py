csvfile = 'LibraryThing_to_OpenLibrary.csv'
db = 'ids.sqlite'

import csv
import string
import sqlite3
import _init_path
from openlibrary.api import OpenLibrary

ol = OpenLibrary('http://openlibrary.org/')
for attempt in range(5):
    try:
        ol.autologin()
        break
    except:
        print 'ol.autologin error, retrying'
conn = sqlite3.connect(db)
c = conn.cursor()
reader = csv.reader(open(csvfile), delimiter='\t', quotechar='|')
for row in reader:
    ltid = row[0]
    olid = row[1]
    key = '/books' + olid[olid.rindex('/'):len(olid)]
    c.execute('select * from ids where key = ?', (key,))
    x = c.fetchone()
    if x != None:
        continue
    print 'Trying to get key: %r' % key
    for attempt in range(5):
        try:
            data = ol.get(key)
            break
        except:
            print 'ol.get() error, retrying'
    if data.has_key('identifiers'):
        if data['identifiers'].has_key('librarything'):
            if data['identifiers']['librarything'].count(ltid) == 0:
                data['identifiers']['librarything'].append(ltid)
        else:
            data['identifiers']['librarything'] = [ltid]
    else:
        data['identifiers'] = {'librarything': [ltid]}
    for attempt in range(5):
        try:
            print ol.save(key, data, 'added LibraryThing ID')
            break
        except:
            print 'ol.save() error; retrying'
    c.execute('insert into ids values (?, ?)', (key, ltid))
    conn.commit()
