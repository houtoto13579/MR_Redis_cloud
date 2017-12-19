import os.path
import sys, time
import csv
import math


def num2suffix(a,l):
    short=False
    k=[]
    for i in range(l):
        k.insert(0,a%5)
        a/=5
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
def getPrefix(suffix,l):
    ans=suffix[:l]
    ans=ans.translate(None, '!@#$')
    return ans

if __name__ == "__main__":
    #fileName = "keys/100k_G_key_39383_new"
    fileName = "keys/100k_key_39005_new"
    file = open(fileName,"r").readlines()
    lengthOfPrefix = (int)(sys.argv[1])
    first=0
    index=0
    last=0
    lengthOfArray = pow(5,lengthOfPrefix)
    outputArray=[]
    prevPrefix=""
    for prefix in range(lengthOfArray):
        thisPrefix = num2suffix(prefix,lengthOfPrefix)
        nextPrefix = num2suffix(prefix+1,lengthOfPrefix)
        if thisPrefix!="X":
            first=index
            while True:
                if index==len(file):
                    last=index
                    break
                linePrefix=getPrefix(file[index].split()[0],lengthOfPrefix)
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
    

    with open("output_test", "wb") as f:
        writer = csv.writer(f,delimiter=' ')
        writer.writerows(outputArray)

