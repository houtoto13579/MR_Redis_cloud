import os.path
import sys, time

if __name__ == "__main__":
    #False mean 0 is the smallest, 63 is biggest
    reduce_file_origin = False
    folderName = "output_TEST/"
    filePrefix = "part-r-"
    print("Checking Correctness under folder " + folderName)
    totalFile = len([name for name in os.listdir('../output_10K/')])
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
            for line in file:
                suffix = line.split()[1]
                if suffix < tmpsuffix:
                    print "Error in file " + fileName
                    sys.exit(0)
                else:
                    tmpsuffix = suffix
            fileCount += addNum
        else:
            print "=== Pass_Testing! ==="
            break
    


