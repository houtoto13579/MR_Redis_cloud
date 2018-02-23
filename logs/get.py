import urllib2
import urllib
from BeautifulSoup import BeautifulSoup
import urlparse
import re
import sys

def checkExist(target,src):
    fileName = target.split('/')[-1]
    if fileName == '':
        return ''
    if fileName in src:
        #print fileName
        return target
    else:
        return ''
def isNotEmpty(s):
    return bool(s and s.strip())
def scrapAllFile(urlStr):
    fileList=[]
    req = urllib2.Request(urlStr)
    try:
        parent_page = urllib2.urlopen(req)
    except urllib2.HTTPError as e:
        return fileList
    soup = BeautifulSoup(parent_page)
    for link in soup.findAll('a'):
        urlLink = link.get('href')
        if isNotEmpty(urlLink):
            urlChild = urlStr + urlLink
            if urlLink[-1] == '/' and urlLink!= '../' and urlLink!= '%03%FF/' and urlLink!='/':
                childFileList = scrapAllFile(urlChild)
                fileList += childFileList
            else:
                fileList.append(urlChild)
    return fileList


if __name__ == "__main__":
    urlStr = "https://www.csie.ntu.edu.tw/~b91082/"
    if len(sys.argv)==2:
        urlStr = sys.argv[1]
    # html_page = urllib2.urlopen("http://ftp.ntu.edu.tw/NTU/course/")
    # soup = BeautifulSoup(html_page)
    # for link in soup.findAll('a'):
    #     print link.get('href')
    print "\n** Initiate Searching Under:" + urlStr
    print ">>Scraping from given url..."
    findList = []
    webFileList = scrapAllFile(urlStr)

    print ">>Finish scraping, start searching..."
    allFileName = "all.txt"
    allFileNameList = []
    #allFileNameList = open(allFileName,"r").readlines()
    with open(allFileName) as f:
        allFileNameList = f.read().splitlines()
    count = 0
    webFileLen = len(webFileList)
    for webFile in webFileList:
        count+=1
        if count%100==0:
            print '>>Searching: (' + str(count)+ '/'+ str(webFileLen) + ')...'
        res = checkExist(webFile,allFileNameList)
        if res != '':
            findList.append(res)
    if len(findList)==0:
        print "\nNothing Found:("
    else:
        print "\n====================="
        print "Here are some result:"
        for findResult in findList:
            print findResult
        print "====================="