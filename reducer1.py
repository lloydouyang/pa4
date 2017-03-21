#!/usr/bin/env python

from operator import itemgetter
import sys

current_id = None
current_count = 0
totalDiff =0
word = None
averageDiffList= [0, 0, 0, 0]
idList= [0,0,0,0]
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    mylist = word.split(",")
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_id == mylist[0]:
        current_count += count
        # print(mylist[0])
        totalDiff += abs(int(mylist[3])-int(mylist[4]))
    else:
        # if current_id:
            # write result to STDOUT
        # print '%s\t%s' % (current_id, totalDiff)
        if (current_count!=0):
            minn=averageDiffList[0]
            minID=0
            for x in range(0,4):
                if (averageDiffList[x]<minn):
                    minn = averageDiffList[x]
                    minID = x
            
            if (minn<(float(totalDiff)/float(current_count))):
                averageDiffList[minID]=float(totalDiff)/float(current_count)
                idList[minID]=current_id

        current_count = count
        current_id = mylist[0]
        totalDiff=abs(int(mylist[3])-int(mylist[4]))


# check the last one
minn=averageDiffList[0]
minID=0
for x in range(0,4):
    if (averageDiffList[x]<minn):
        minn = averageDiffList[x]
        minID = x
                
if (minn<(float(totalDiff)/float(current_count))):
    averageDiffList[minID]=float(totalDiff)/float(current_count)
    idList[minID]=current_id

print("Below are the 4 worst HVAC systems(not in a sorted order):")
print(idList)
print("Below are the their corresponding daily differences between actual and desired temperatures")
print(averageDiffList)