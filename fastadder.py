#!/usr/bin/python

# IdentifierBot
# by Daniel Montalvo

csvfile = 'LibraryThing_to_OpenLibrary.csv'
batch_size = 500

import traceback
import csv
import string
import _init_path
import sys

sys.path.append('/petabox/sw/lib/python')
from openlibrary.api import OpenLibrary, marshal

ol = OpenLibrary("http://openlibrary.org")
reader = csv.reader(open(csvfile), delimiter='\t', quotechar='|')
f = open('authors.txt', 'w')

def fix_toc(doc):
     doc = marshal(doc)

     def f(d):
         """function to fix one toc entry."""
         if d.get('type') == '/type/text':
             return dict(title=d['value'])
         else:
             return d

     toc = doc.get('table_of_contents')
     if toc:
          if type(toc) == dict:
               doc['table_of_contents'] = [f(x) for x in toc]
     return doc

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
    sys.exit('Failed to log in.')

# Go through the csv file in batches until done
batch_count = 0
done = False
while not done:
    olids = []
    ltids = []
    iddict = {}
    data = []
    batch_count += 1
    print 'Starting batch %r.' % batch_count

    # Get a batch of keys from the file
    for a in range(batch_size):
        try:
            row = reader.next()
        except:
            done = True
            print 'Finished reading from csv file.'
            break
        olid = row[1]
        key = '/books' + olid[olid.rindex('/'):len(olid)]
        olids.append(key)
        iddict[key] = row[0]

    # If the whole batch has been done already, skip to the next batch
    print olids
    if len(olids) == 0:
        print 'Batch %r already done; skipping.' % batch_count
        continue

    # Fetch the book data from the site
    got_data = False
    for attempt in range(5):
         try:
              data = ol.get_many(olids)
              got_data = True
              break
         except KeyboardInterrupt:
              sys.exit(0)
         except:
              print 'ol.get_many() error'
              traceback.print_exc(file=sys.stdout)
    if not got_data:
        sys.exit('Failed to get data.')
    datalist = []
    for doc_key in data:
         datalist.append(data[doc_key])

    # Fix toc errors
    datalist = [fix_toc(doc) for doc in datalist]
    
    # Add the ids to the metadata
    newlist = []
    for book in datalist:
        newbook = True
        ltid = iddict[book['key']]
        if book.has_key('identifiers'):
            if book['identifiers'].has_key('librarything'):
                if book['identifiers']['librarything'].count(ltid) == 0:
                    book['identifiers']['librarything'].append(ltid)
                else:
                    newbook = False
            else:
                book['identifiers']['librarything'] = [ltid]
        else:
            book['identifiers'] = {'librarything': [ltid]}
        if newbook:
            newlist.append(book)

    # Save the data back to the site
    # If there's nothing to update, skip save_many
    if len(newlist) == 0:
         continue
    saved = False
    #for attempt in range(5):
    try:
         print 'Trying to save_many'
         print ol.save_many(newlist, 'added LibraryThing ID')
         saved = True
    except KeyboardInterrupt:
         sys.exit(0)
    except:
         badlist = []
         for e in newlist:
              for akey in e.get('authors', []):
                   got_author = False
                   for attempt in range(5):
                        try:
                             a = ol.get(akey['key'])
                             got_author = True
                             break
                        except:
                             print 'ol.get(author) error; retrying.'
                   if not got_author:
                        sys.exit('Failed to get author: %r' % akey['key'])
                   if a['type'] == '/type/author':
                        continue
                   if badlist.count(e) == 0:
                        badlist.append(e)
         for badbook in badlist:
              newlist.remove(badbook)
              f.write(badbook['key'])
              f.write('\n')
         print ol.save_many(newlist, 'added LibraryThing ID')
