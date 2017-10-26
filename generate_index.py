import os.path
import sys, time
import numpy
import csv


def num2suffix(a):
    short=False
    k=[]
    k.insert(0,a%5)
    a/=5
    k.insert(0,a%5)
    a/=5
    k.insert(0,a%5)
    a/=5
    k.insert(0,a%5)
    #print(k)
    ans=""
    for element in k:
        if short==True and element!=0:
            return "X"
        if element==1:
            ans+='A'
        elif element==2:
            ans+='C'
        elif element==3:
            ans+='G'
        elif element==4:
            ans+='T'
        elif element==0:
            short=True
    return ans
def getPrefix(suffix):
    ans=suffix[:4]
    ans=ans.translate(None, '!@#$')
    return ans

if __name__ == "__main__":
    #False mean 0 is the smallest, 63 is biggest

    fileName = "100k_key_39005"
    file = open(fileName,"r").readlines()
    
    first=0
    index=0
    last=0
    outputArray=[]
    prevPrefix=""
    for prefix in range(625):
        thisPrefix = num2suffix(prefix)
        nextPrefix = num2suffix(prefix+1)
        if thisPrefix!="X":
            first=index
            while True:
                if index==len(file):
                    last=index
                    break
                linePrefix=getPrefix(file[index].split()[1])
                if linePrefix!=thisPrefix:
                     last=index
                     break
                index+=1
            #print thisPrefix,first,last
            outputArray.append([first,last])
        else:
            outputArray.append([0,0])
        prevPrefix=thisPrefix
            #print(file[index].split()[1])
            
    #print outputArray[531]


    with open("fast_index", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(outputArray)

