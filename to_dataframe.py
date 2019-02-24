# -*- coding: utf-8 -*
from pyspark import SparkConf, SparkContext
import csv
import os
import numpy as np

#from pyspark.sql import SparkSession
#from pyspark.sql import Row
#import collections

os.environ['PYTHONPATH']='python3'
conf = SparkConf().setMaster("local").setAppName("cal_position")
sc = SparkContext(conf = conf)

IDs  =[1,10,19,28,37]
#heros=[2,9,16,23,30]
#Ks   =[2,9,16,23,30]
#Ds   =[2,9,16,23,30]
#Asis =[2,9,16,23,30]
#Gs   =[2,9,16,23,30]
#Lvls =[2,9,16,23,30]


def what_position(line):
    fields=line.split(',')
    #get if win
    ifwin=fields[8]
    #fields=np.array(fields)
    P_1=fields[3]*3-fields[4]*5+fields[5]+fields[6]*3+fields[7]*5
    P_2=fields[12]*3-fields[13]*5+fields[14]+fields[15]*3+fields[16]*5
    P_3=fields[21]*3-fields[22]*5+fields[23]+fields[24]*3+fields[25]*5
    P_4=fields[30]*3-fields[31]*5+fields[32]+fields[33]*3+fields[34]*5
    P_5=fields[39]*3-fields[40]*5+fields[41]+fields[42]*3+fields[43]*5

    sort_rank={"P_1":P_1,"P_2":P_2,"P_3":P_3,"P_4":P_4,"P_5":P_5}
    sort_list=sorted(sort_rank.items(),key= lambda x:x[1])

    out1=sort_list.index(("P_1",P_1))+1
    out1=[fields[IDs[0]],out1,ifwin]

    out2=sort_list.index(("P_2",P_2))+1
    out2=[fields[IDs[1]],out2,ifwin]

    out3=sort_list.index(("P_3",P_3))+1
    out3=[fields[IDs[2]],out3,ifwin]

    out4=sort_list.index(("P_4",P_4))+1
    out4=[fields[IDs[3]],out4,ifwin]

    out5=sort_list.index(("P_5",P_5))+1
    out5=[fields[IDs[4]],out5,ifwin]

    #return (out1,out2,out3,out4,out5)
    return (out1+out2+out3+out4+out5)

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def count_number(sample):
    sample=[sample[0],sample[1]]
    sample=toCSVLine(sample)
    return(sample,1)

def cal_winrate(x,y):
    if x<=y:
        return (100*x/y)
    else:
        return (100*y/x)

def turn_tuple(x):
    first_half=list(x[0])
    first_half.append(x[1])
    return (first_half)

lines = sc.textFile("position_origin.txt")
parsedLines = lines.map(what_position)

lines=parsedLines.map(lambda x:(x[0],x[1],x[2]))
line2=parsedLines.map(lambda x:(x[3],x[4],x[5]))
line3=parsedLines.map(lambda x:(x[6],x[7],x[8]))
line4=parsedLines.map(lambda x:(x[9],x[10],x[11]))
line5=parsedLines.map(lambda x:(x[12],x[13],x[14]))
lines=lines.union(line2).union(line3).union(line4).union(line5)



for_winandlose=lines.map(lambda x:(tuple([x[0],x[1]]),1))
for_winandlose=for_winandlose.reduceByKey(lambda x,y:x+y)

#for_winonly=lines.map(lambda x:x[2])
for_winonly=lines.filter(lambda x:x[2] == ' 1')
for_winonly=for_winonly.map(lambda x:(tuple([x[0],x[1]]),1))
for_winonly=for_winonly.reduceByKey(lambda x,y:x+y)

lines=for_winandlose.union(for_winonly)
lines=lines.reduceByKey(cal_winrate)
lines=lines.map(turn_tuple)

lines=lines.map(toCSVLine)
results = lines.collect()
#results = lines.collect()
print(results[0])

with open('theposition.txt', 'w') as f:
    for item in results:
        f.write("%s\n" % item)
