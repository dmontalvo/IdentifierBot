csvfile = 'LibraryThing_to_OpenLibrary.csv'
#db = 'ids.sqlite'

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
        print 'ol.autologin() error; retrying'
#conn = sqlite3.connect(db)
#c = conn.cursor()
reader = csv.reader(open(csvfile), delimiter='\t', quotechar='|')
done = False
for a in range(2):
    olids = []
    ltids = []
    iddict = {}
    data = []
    for b in range(2):
        try:
            row = next(reader)
        except:
            done = True
            break
        olid = row[1]
        key = '/books' + olid[olid.rindex('/'):len(olid)]
        #c.execute('select * from ids where key = ?', (key,))
        #x = c.fetchone()
        #if x != None:
            #continue
        olids.append(key)
        iddict[key] = row[0]
    for attempt in range(5):
        try:
            data = ol.get_many(olids)
            break
        except:
            print 'ol.get_many() error; retrying'
    keys = []
    for book in data:
        key = book['key']
        keys.append(key)
        ltid = iddict[key]
        if book.has_key('identifiers'):
            if book['identifiers'].has_key('librarything'):
                if book['identifiers']['librarything'].count(ltid) == 0:
                    book['identifiers']['librarything'].append(ltid)
            else:
                book['identifiers']['librarything'] = [ltid]
        else:
            book['identifiers'] = {'librarything': [ltid]}
    #for attempt in range(5):
        #try:
            #print ol.save_many(data, 'added LibraryThing ID')
            #break
        #except:
            #print 'ol.save_many() error; retrying'
    #c.execute('insert into ids values (?, ?)', (key, ltid))
    #conn.commit()
    if done:
        break
