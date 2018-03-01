import urllib2
import urllib
from BeautifulSoup import BeautifulSoup
import urlparse
import re
import sys
import time
from datetime import datetime
from selenium import webdriver

def scrapAllFile(urlStr, browser, interval):
    # req = urllib2.Request(urlStr)
    # try:
    #     parent_page = urllib2.urlopen(req)
    # except urllib2.HTTPError as e:
    #     return "404"
    # soup = BeautifulSoup(parent_page)
    
    browser.get(urlStr)
    browser.refresh()
    time.sleep(interval)
    browser.implicitly_wait(10)
    html = browser.page_source
    soup = BeautifulSoup(html)

    csvString = ""
    count = 0
    # print soup
    allTable = soup.findAll('table')
    # print allTable
    rowList = allTable[1].findAll("tr")
    row=rowList[1]
    itemList = row.find('td').contents[0].split()
    
    csvString+=itemList[0]+','+itemList[2]+','
    
    intermediate_row=rowList[2]
    intermediate_itemList = intermediate_row.find('td').contents[0].split()
    csvString+=intermediate_itemList[0]+','

    #print csvString
    return csvString


if __name__ == "__main__":
    urlStr = "http://140.109.17.134:50070/dfshealth.html#tab-overview"
    outputName = "testLog.csv"
    interval = 60
    if len(sys.argv)==2:
        #urlStr = sys.argv[1]
        outputName = sys.argv[1]
    print "\n**** Initiate Scraping ****"
    print ""
    print "* url: " + urlStr
    print "* interval: "+ str(interval) + ' sec'
    print "* outputFile: "+ outputName
    print "\n"
    print ">>Start scraping..."

    browser = webdriver.Firefox(executable_path = '/usr/bin/geckodriver')
    browser.implicitly_wait(10)
    while(True):
        print ">>Scrapt in "+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'...'
        csvString = scrapAllFile(urlStr,browser,interval)
        csvString += str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        csvString += '\n'
        if(csvString != "404"):
            with open(outputName,'a') as outfile:
                outfile.write(csvString)
            #print "write"
        else:
            print ">>Return... 404"
            break
        # time.sleep(interval)
    print "Finish"