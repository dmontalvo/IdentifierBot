csvfile = 'ltids.csv'
db = 'ids.sqlite'

import csv
import string
import sqlite3
import _init_path
from openlibrary.api import OpenLibrary

ol = OpenLibrary('http://0.0.0.0:8080')
ol.autologin()
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
    data = ol.get(key)
    if data.has_key('identifiers'):
        if data['identifiers'].has_key('librarything'):
            if data['identifiers']['librarything'].count(ltid) == 0:
                data['identifiers']['librarything'].append(ltid)
        else:
            data['identifiers']['librarything'] = [ltid]
    else:
        data['identifiers'] = {'librarything': [ltid]}
    print data
    #ol.save(key, data, 'Testing IdentifierBot')  
    c.execute('insert into ids values (?, ?)', (key, ltid))
    conn.commit()
