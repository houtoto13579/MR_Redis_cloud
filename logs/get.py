import urllib2
import urllib
from BeautifulSoup import BeautifulSoup
import urlparse
import re
import sys
import time
from datetime import datetime


def scrapAllFile(urlStr):
    req = urllib2.Request(urlStr)
    try:
        parent_page = urllib2.urlopen(req)
    except urllib2.HTTPError as e:
        return "404"
    soup = BeautifulSoup(parent_page)
    
    csvString = ""
    count = 0
    allTable = soup.find('table',{"id":"counters"})
    tableList = allTable.findAll("td",{"class":"table"})
    for table in tableList:
        csvString += '\n'
        for td in table.findAll('td'):
            if(count%4 == 0):
                #print td.find('a').contents[0].strip()
                title = td.find('a').contents[0].strip().replace(',', '')
                csvString += title +','
            elif (count%4 == 3):
                counterRecord = td.contents[0].strip().replace(',', '')
                csvString += counterRecord+'\n'
            else:
                counterRecord = td.contents[0].strip().replace(',', '')
                csvString += counterRecord+','
            count+=1
    # print csvString
    return csvString


if __name__ == "__main__":
    urlStr = "http://140.109.17.134:9046/proxy/application_1517129818535_0006/mapreduce/jobcounters/job_1517129818535_0006"
    outputName = "testLog.csv"
    interval = 60
    if len(sys.argv)==3:
        urlStr = sys.argv[1]
        outputName = sys.argv[2]
    # html_page = urllib2.urlopen("http://ftp.ntu.edu.tw/NTU/course/")
    # soup = BeautifulSoup(html_page)
    # for link in soup.findAll('a'):
    #     print link.get('href')
    print "\n**** Initiate Scraping ****"
    print ""
    print "* url: " + urlStr
    print "* interval: "+ str(interval) + ' sec'
    print "* outputFile: "+ outputName
    print "\n"
    print ">>Start scraping..."


    while(True):
        print ">>Scrapt in "+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'...'
        csvString = scrapAllFile(urlStr)
        if(csvString != "404"):
            with open(outputName,'w') as outfile:
                outfile.write(csvString)
        else:
            print ">>Return... 404"
            break
        time.sleep(interval)
    print "Finish"