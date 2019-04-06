from pyspark import SparkConf, SparkContext
import json
import os
import ast
import csv
from boto.s3.connection import S3Connection
from psycopg2 import extras

os.environ['PYTHONPATH']='python3'
conf = SparkConf().setMaster("spark://ec2-3-90-122-232.compute-1.amazonaws.com:7077").setAppName("groupplayerhistory")
sc = SparkContext(conf = conf)

#read the json which contains players
def readjson(jfile):
    eachj=ast.literal_eval(jfile)
    return (
[eachj["0"]["account_id"],'r'],\
[eachj["1"]["account_id"],'r'],\
[eachj["2"]["account_id"],'r'],\
[eachj["3"]["account_id"],'r'],\
[eachj["4"]["account_id"],'r'],\
[eachj["128"]["account_id"],'d'],\
[eachj["129"]["account_id"],'d'],\
[eachj["130"]["account_id"],'d'],\
[eachj["131"]["account_id"],'d'],\
[eachj["132"]["account_id"],'d'])

#further parse
def parseLine(a_filt):
    pgroup=a_filt[-1]
    pgroup = readjson(pgroup)
    line=list(a_filt)[:-1]
    temp=line[1:]

    if line[0]=="t":

        a0=temp+[pgroup[0][0],pgroup[0][1],"t"]
        a1=temp+[pgroup[1][0],pgroup[1][1],"t"]
        a2=temp+[pgroup[2][0],pgroup[2][1],"t"]
        a3=temp+[pgroup[3][0],pgroup[3][1],"t"]
        a4=temp+[pgroup[4][0],pgroup[4][1],"t"]
        a5=temp+[pgroup[5][0],pgroup[5][1],"f"]
        a6=temp+[pgroup[6][0],pgroup[6][1],"f"]
        a7=temp+[pgroup[7][0],pgroup[7][1],"f"]
        a8=temp+[pgroup[8][0],pgroup[8][1],"f"]
        a9=temp+[pgroup[9][0],pgroup[9][1],"f"]

    else:

        a0=temp+[pgroup[0][0],pgroup[0][1],"f"]
        a1=temp+[pgroup[1][0],pgroup[1][1],"f"]
        a2=temp+[pgroup[2][0],pgroup[2][1],"f"]
        a3=temp+[pgroup[3][0],pgroup[3][1],"f"]
        a4=temp+[pgroup[4][0],pgroup[4][1],"f"]
        a5=temp+[pgroup[5][0],pgroup[5][1],"t"]
        a6=temp+[pgroup[6][0],pgroup[6][1],"t"]
        a7=temp+[pgroup[7][0],pgroup[7][1],"t"]
        a8=temp+[pgroup[8][0],pgroup[8][1],"t"]
        a9=temp+[pgroup[9][0],pgroup[9][1],"t"]

    return (a0+a1+a2+a3+a4+a5+a6+a7+a8+a9)

#intial parse
def filterparse(fields):
    match_id = fields[0]
    radiant_win = fields[2]
    human_players = fields[12]
    game_mode = fields[16]
    mystr=','.join(fields[26:56]).replace('""','"')[1:-1]

    return (radiant_win, match_id, human_players, game_mode, mystr)

lines = sc.textFile("s3a://chaopeiinsight.myfirstbucket/example/matchtest")
filt_by_len=lines.map(lambda x:x.split(','))
after_by_len=filt_by_len.filter(lambda x:len(x)==56)
firstparsed = after_by_len.map(filterparse)
filtered=firstparsed.filter(lambda x: x[2]=="10").filter(lambda x:x[3]=='1').map(lambda x:x[:2]+x[4:])
finalgroup=filtered.map(parseLine).map(lambda x:','.join(str(i) for i in x))
results = finalgroup.collect()

def rearange(results):
    i,res=1,[]
    while results[i]:
        res.append(result[i]+result[i+1]+result[i+2]+result[i+3]+result[i+4])
    return res

results = rearange(results)

with open("raw_match.csv","w") as newfile:
    csv_writer=csv.writer(newfile, delimiter=" ")
    for line in results:
        csv_writer.writerow(line)
