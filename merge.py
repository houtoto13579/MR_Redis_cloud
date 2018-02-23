import os.path
import sys, time

if __name__ == "__main__":
    #False mean 0 is the smallest, 63 is biggest
    reduce_file_origin = False
    folderName = "../output/output_eel_keyCount/"
    outputName = "../output/merge_eel_keyCount"
    filePrefix = "part-r-"
    print("Merging  under folder " + folderName)
    fileCount = 0
    addNum=1
    if reduce_file_origin:
        fileCount = 63
        addNum=-1        
    tmpsuffix = "$"
    while(True):
        fileProfix = str(fileCount).zfill(5)
        fileName = folderName + filePrefix + fileProfix
        if os.path.isfile(fileName):
            file = open(fileName,"r") 
	    lineCount=0
            with open(outputName,'a') as outfile:
            	for line in file:
		            outfile.write(line)
	    fileCount+=addNum
        else:
            print "=== Finish Merging! ==="
            break
    


