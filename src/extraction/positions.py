# -*- coding: utf-8 -*
from pyspark import SparkConf, SparkContext
import csv
import os
import numpy as np
import psycopg2
from boto.s3.connection import S3Connection
from psycopg2 import extras

os.environ['PYTHONPATH']='python3'
conf = SparkConf().setMaster("spark://ec2-3-90-122-232.compute-1.amazonaws.com:7077").setAppName("groupplayerhistory")
sc = SparkContext(conf = conf)


#account_id = [4,13,22,31,40]
#Kills   = [5,14,23,32,41]
#Deaths   = [6,15,24,33,42]
#assistsis = [7,16,25,34,43]
#gold_per_min = [8,17,26,35,44]
#xp_per_min = [9,18,27,36,45]

#ouput aggregated positions
def what_position(line):
    fields=line.split(',')
    #get if win
    ifwin=fields[8]
    #fields=np.array(fields)
    score=[]
    s=5
    for i in range(5):
        each_score = fields[5]*3+fields[6]*5+fields[7]+fields[8]*3+fields[9]*5
        s+=9
    P_1=score=[0]
    P_2=score=[1]
    P_3=score=[2]
    P_4=score=[3]
    P_5=score=[4]

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

    return (out1+out2+out3+out4+out5)

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def count_number(sample):
    sample=[sample[0],sample[1]]
    sample=toCSVLine(sample)
    return(sample,1)

def turn_tuple(x):
    first_half=list(x[0])
    first_half.append(x[1])
    return (first_half)

#insert to database
def insert_db(data):
    psql_credeintal = {
        'database': 'wode',
        'user': 'wode',
        'password': '***',
        'host': '54.242.73.153',
        'port': '5432'

    psql_db_conn = psycopg2.connect(**psql_credeintal)

    global psql_db_conn
    psql_db_cur = psql_db_conn.cursor()
    psql_db_cur.execute('PREPARE inserts AS INSERT INTO "positions" (user_id, position, winrate) \
                                            VALUES ($1, $2, $3);')
    extras.execute_batch(psql_db_cur, "EXECUTE inserts (%s, %s, %s)", [data])
    psql_db_cur.execute("DEALLOCATE inserts")
    psql_db_conn.commit()

def __name__=="__main__":
    #connect AWS
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    conn = S3Connection(aws_access_key,aws_secret_access_key)
    bucket=conn.get_bucket("chaoinsight")
    lines = sc.textFile("s3a://chaoinsight/position_origin.txt")

    parsedLines = lines.map(what_position)
    lines=parsedLines.map(lambda x:(x[0],x[1],x[2]))
    line2=parsedLines.map(lambda x:(x[3],x[4],x[5]))
    line3=parsedLines.map(lambda x:(x[6],x[7],x[8]))
    line4=parsedLines.map(lambda x:(x[9],x[10],x[11]))
    line5=parsedLines.map(lambda x:(x[12],x[13],x[14]))
    lines=lines.union(line2).union(line3).union(line4).union(line5)

    for_winandlose=lines.map(lambda x:(tuple([x[0],x[1]]),1))
    for_winandlose=for_winandlose.reduceByKey(lambda x,y:x+y)

    for_winonly=lines.filter(lambda x:x[2] == ' 1')
    for_winonly=for_winonly.map(lambda x:(tuple([x[0],x[1]]),1))
    for_winonly=for_winonly.reduceByKey(lambda x,y:x+y)

    lines=for_winandlose.union(for_winonly)
    lines=lines.reduceByKey(cal_winrate)
    lines=lines.map(turn_tuple)

    lines_csv=lines.map(toCSVLine)
    lines.foreach(insert_db)
