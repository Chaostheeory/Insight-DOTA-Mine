from pyspark import SparkConf, SparkContext
import json
import os
import ast
import csv
#import boto3
#from boto.s3.connection import S3Connection

os.environ['PYTHONPATH']='python3'
conf = SparkConf().setMaster("local").setAppName("groupplayerhistory")
#SparkContext.stop()

#conf = SparkConf().setMaster("spark://ip-10-0-0-14:7077").setAppName("groupplayerhistory")
sc = SparkContext(conf = conf)

def readjson(jfile):
    eachj=ast.literal_eval(jfile)
#    return (eachj)

    return ([eachj["0"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["1"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["2"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["3"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["4"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["128"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["129"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["130"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["131"]["account_id"],eachj["0"]["hero_id"]],\
[eachj["132"]["account_id"],eachj["0"]["hero_id"]])

def parseLine(a_filt):
    pgroup=a_filt[-1]
    pgroup = readjson(pgroup)
    line=list(a_filt)[:-1]

    a0=line
    a1=line
    a2=line
    a3=line
    a4=line
    a5=line
    a6=line
    a7=line
    a8=line
    a9=line
    if line[1]=="t":

            a0+=[pgroup[0][0],pgroup[0][1],"t"]
            a1+=[pgroup[1][0],pgroup[1][1],"t"]
            a2+=[pgroup[2][0],pgroup[2][1],"t"]
            a3+=[pgroup[3][0],pgroup[3][1],"t"]
            a4+=[pgroup[4][0],pgroup[4][1],"t"]
            a5+=[pgroup[5][0],pgroup[5][1],"f"]
            a6+=[pgroup[6][0],pgroup[6][1],"f"]
            a7+=[pgroup[7][0],pgroup[7][1],"f"]
            a8+=[pgroup[8][0],pgroup[8][1],"f"]
            a9+=[pgroup[9][0],pgroup[9][1],"f"]

    else:
            a0+=[pgroup[0][0],pgroup[0][1],"f"]
            a1+=[pgroup[1][0],pgroup[1][1],"f"]
            a2+=[pgroup[2][0],pgroup[2][1],"f"]
            a3+=[pgroup[3][0],pgroup[3][1],"f"]
            a4+=[pgroup[4][0],pgroup[4][1],"f"]
            a5+=[pgroup[5][0],pgroup[5][1],"t"]
            a6+=[pgroup[6][0],pgroup[6][1],"t"]
            a7+=[pgroup[7][0],pgroup[7][1],"t"]
            a8+=[pgroup[8][0],pgroup[8][1],"t"]
            a9+=[pgroup[9][0],pgroup[9][1],"t"]

    return (a0,a1,a2,a3,a4,a5,a6,a7,a8,a9)


def filterparse(fields):
    match_id = fields[0]
    radiant_win = fields[2]
    start_time = fields[3]
    duration = fields[4]
    human_players = fields[12]
    game_mode = fields[16]
    mystr=','.join(fields[26:56]).replace('""','"')[1:-1]

    return (match_id, radiant_win, start_time, duration, human_players, game_mode, mystr)


#bucket=conn.get_bucket("chaopeiinsight.myfirstbucket/example")
def split_filt(lines):
    out_put=lines.split(',')
    return (out_put)

#lines = sc.textFile("s3a://chaopeiinsight.myfirstbucket/example/matchtest")
lines = sc.textFile("matchtest")
filt_by_len=lines.map(split_filt)
after_by_len=filt_by_len.filter(lambda x:len(x)==56)
firstparsed = after_by_len.map(filterparse)
filtered=firstparsed.filter(lambda x: x[4]=="10")
finalgroup=filtered.map(parseLine)
results = finalgroup.collect();

with open("newcsv","w") as newfile:
    csv_writer=csv.writer(newfile, delimiter=";")
    for line in results:
        csv_writer.writerow(line)

for result in results:
#
    print (result)
