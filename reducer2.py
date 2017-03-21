#!/usr/bin/env python

from operator import itemgetter
import sys

current_id = None
current_time = None
current_count = 0
totalTemp = 0
word = None
averageTempList = [120, 120, 120, 120]
idList = [0, 0, 0, 0]
listoflists = [[],[],[],[]]
listoftimes = [[],[],[],[]]
a_list = []
t_list = []
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
    if (current_count == 0):
        current_count = count
        current_id = mylist[0]
        current_time = mylist[1]
        totalTemp += int(mylist[5])
    else:
        if ((current_id == mylist[0]) & (current_time == mylist[1])):
            current_count += count
            totalTemp += int(mylist[5])
        else:
            if current_id == mylist[0]:
                if (current_time != mylist[1]):
                    a_list.append(float(totalTemp) / float(current_count))
                    t_list.append(current_time)
                    current_count = count
                    current_time = mylist[1]
                    totalTemp = int(mylist[5])
            else:
                maxx = averageTempList[0]
                maxxID = 0
                for x in range(0, 4):
                    if (averageTempList[x] > maxx):
                        maxx = averageTempList[x]
                        maxxID = x
                
                a_list.append(float(totalTemp) / float(current_count))
                t_list.append(current_time)
                av = sum(a_list) / len(a_list)
               
                # print(current_id,current_count, sum(a_list),len(a_list),av)
                if (maxx > av):
                    averageTempList[maxxID] = av
                    idList[maxxID] = current_id
                    listoflists[maxxID]=a_list
                    listoftimes[maxxID]=t_list
                a_list = []
                t_list = []
                current_count = count
                current_id = mylist[0]
                current_time = mylist[1]
                totalTemp = int(mylist[5])


# do not forget to output the last word if needed!
# if current_id == word:
#     print '%s\t%s' % (current_word, totalTemp)
maxx=averageTempList[0]
maxxID=0
for x in range(0,4):
    if (averageTempList[x]>maxx):
        maxx = averageTempList[x]
        maxxID = x
a_list.append(float(totalTemp) / float(current_count))
t_list.append(current_time)
av = sum(a_list)/len(a_list)
# print(current_id,current_count, sum(a_list),len(a_list),av)
if (maxx>av):
    averageTempList[maxxID]=av
    idList[maxxID]=current_id
    listoflists[maxxID]=a_list
    listoftimes[maxxID]=t_list
a_list = []
t_list = []
current_count = count
current_id = mylist[0]
current_time= mylist[1]
totalTemp = int(mylist[5])
# print(current_id)
print("Below are the 4 coldest buildings(not in a sorted order):")
print(idList)
print("Below are the their corresponding daily average temperatures:")
print(averageTempList)
print("Below are the 4 buildings with their average temperatures and corresponding time of day:")
for i in range(0,4):
    for j in range(0,len(listoflists[i])):
        print '%s\t%s\t%s' % ("Building "+idList[i],listoflists[i][j],listoftimes[i][j])
    

