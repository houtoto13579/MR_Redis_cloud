import os.path
import sys, time

if __name__ == "__main__":
    #fileName = "keys/100k_G_key_39383"
    fileName = "keys/100k_key_39005"
    outputFileName = fileName + '_new'
    print("Update old key type to new key type, fileName:" + fileName)
    lineCount = 0
    tmpLine = "$"
    start_index = 0
    if os.path.isfile(fileName):
        file = open(fileName,"r") 
        with open('../merge_grouper_key','a') as outfile:
            for line in file:
                suffix = line.split()[1]
                if tmpLine==suffix:
                    lineCount+=1
                else:
                    tmpLine= tmpLine+' '+str(start_index)+' '+str(lineCount-1)+'\n'
                    with open(outputFileName,'a') as outfile:
                        outfile.write(tmpLine)
                    start_index = lineCount
                    tmpLine = suffix
                    lineCount+=1
            tmpLine= tmpLine+' '+str(start_index)+' '+str(lineCount-1)+'\n'
            with open(outputFileName,'a') as outfile:
                outfile.write(tmpLine)
    else:
        print "no file detected"