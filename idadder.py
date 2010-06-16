csvfile = 'ids.csv'

import csv
reader = csv.reader(open(csvfile), delimiter=' ', quotechar='|')
for row in reader:
    print row[0]
    print row[1]
