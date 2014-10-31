import psycopg2
from datetime import datetime


conn = psycopg2.connect("dbname=nsl_apps user=nsl_apps password=Databas3")
cur = conn.cursor()

#cur.execute('TRUNCATE TABLE outages_summary CASCADE')
#conn.commit()

cur.execute('SELECT timestamp, sum(outage) FROM outages_ace GROUP BY timestamp')
acelist = cur.fetchall()

cur.execute('SELECT timestamp, sum(outage) FROM outages_jcpl GROUP BY timestamp')
jcpllist = cur.fetchall()

cur.execute('SELECT timestamp, sum(outage) FROM outages_pseg GROUP BY timestamp')
pseglist = cur.fetchall()

timestamps = []

for ace in acelist:
   timestamps.append(ace[0])
for jcpl in jcpllist:
   timestamps.append(jcpl[0])
for pseg in pseglist:
   timestamps.append(pseg[0])
print len(timestamps)
timestamps = set(timestamps)
print len(timestamps)

for timestamp in timestamps:
    
