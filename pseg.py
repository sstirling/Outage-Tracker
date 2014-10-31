import time 
import requests
from bs4 import BeautifulSoup
import json
import sys
from datetime import datetime
import psycopg2

company = "PSE&G"

conn = psycopg2.connect("dbname = nsl_apps user = nsl_apps password = Databas3")
cur = conn.cursor()

currenttime = datetime.strptime(" ".join(sys.argv[1:]),'%a %b %d %H:%M:%S %Z %Y')

timestamp = int(time.time()*1000)

xmlurl = "http://outagecenter.pseg.com/data/customlayers/informationpoint/metadata.xml?timestamp=%s" % timestamp

metadata = requests.get(xmlurl).text

soup = BeautifulSoup(metadata)

urlfragment = soup.find("directory").text

updatetime = datetime.strptime(" ".join(urlfragment.split("_")),"%Y %m %d %H %M %S")

dataurl = "http://outagecenter.pseg.com/data/interval_generation_data/%s/report.js?timestamp=%s" % (urlfragment, timestamp)

data = requests.get(dataurl).text

json = json.loads(data)

counties = json['file_data']['curr_custs_aff']['areas'][0]['areas'][0]['areas']

for c in counties: 
    county = c["area_name"].title()
    for town in c["areas"]:
        name = town["area_name"].title()
        outages = town["custs_out"]
        cur.execute("INSERT INTO outages_pseg(company, updated, timestamp, county, muni, outage) VALUES(%s,%s,%s,%s,%s,%s)", (company, updatetime, currenttime, county, name, outages))
        conn.commit()
        cur.execute("INSERT INTO outages_summary(timestamp, pseg) VALUES(%s, %s)", (currenttime, outages))
        conn.commit()

cur.close()
conn.close()
