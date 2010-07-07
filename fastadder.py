#!/usr/bin/python

# IdentifierBot
# by Daniel Montalvo

csvfile = 'LibraryThing_to_OpenLibrary.csv'
db = 'ids.sqlite'
batch_size = 10

import csv
import string
import sqlite3
import _init_path
import sys
from openlibrary.api import OpenLibrary

ol = OpenLibrary("http://anand.openlibrary.org")
conn = sqlite3.connect(db)
c = conn.cursor()
reader = csv.reader(open(csvfile), delimiter='\t', quotechar='|')

# Log in
logged_in = False
for attempt in range(5):
    try:
        ol.autologin()
        logged_in = True
        break
    except:
        print 'ol.autologin() error; retrying'
if not logged_in:
    sys.exit("Failed to log in.")

# Go through the csv file in batches until done
done = False
while not done:
    olids = []
    ltids = []
    iddict = {}
    data = []

    # Get a batch of keys from the file
    for a in range(batch_size):
        try:
            row = next(reader)
        except:
            done = True
            break
        olid = row[1]
        key = '/books' + olid[olid.rindex('/'):len(olid)]

        # If the book has already been updated, skip it
        c.execute('select * from ids where key = ?', (key,))
        x = c.fetchone()
        if x is not None:
            continue

        olids.append(key)
        iddict[key] = row[0]

    # Fetch the book data from the site
    for attempt in range(5):
        try:
            data = ol.get_many(olids)
            break
        except:
            print 'ol.get_many() error; retrying'

    # Add the ids to the metadata
    for book in data:
        ltid = iddict[book['key']]
        if book.has_key('identifiers'):
            if book['identifiers'].has_key('librarything'):
                if book['identifiers']['librarything'].count(ltid) == 0:
                    book['identifiers']['librarything'].append(ltid)
            else:
                book['identifiers']['librarything'] = [ltid]
        else:
            book['identifiers'] = {'librarything': [ltid]}

    # Save the data back to the site
    for attempt in range(5):
        try:
            print ol.save_many(data, 'added LibraryThing ID')
            break
        except:
            print 'ol.save_many() error; retrying'

    # Add the batch to the sqlite database
    for k in iddict:
        c.execute('insert into ids values (?, ?)', (k, iddict[k]))
        conn.commit()
