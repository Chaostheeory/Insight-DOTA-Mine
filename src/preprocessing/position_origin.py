import pandas as pd
import random
from random import randint
from random import sample
from random import choice
import os
import csv
os.chdir("/home/ubuntu/mygit")

#matchtest=pd.read_csv("matchesdota")
#playertest=pd.read_csv("playertest")

stats=pd.read_csv("stat_pool2.txt")
#stats=pd.read_csv("stats_pool.txt")

heroid=list(stats["hero_id"])
kills=list(stats["kills"])
deaths=list(stats["deaths"])
assists=list(stats["assists"])
gold=list(stats["gold"])
levels=list(stats["level"])

#create M_id pool
#start with ID: 2304335744

P_pool=[]
iniP=119807323
for i in range(2000000):
    iniP+=randint(0, 4)
    P_pool.append(iniP)

#create Stats pool

#win true false list
wins=[[1,1,1,1,1,0,0,0,0,0],[0,0,0,0,0,1,1,1,1,1]]

#create table with one player each row
sr_table=[]
iniM_id=2310333399
M_id=iniM_id
for i in range(1000000):
    M_id+=randint(0, 3)
    p_per_match=sample(P_pool,10)
    winstat=choice(wins)
    w=0
    for j in p_per_match:
        o_reco=[]
        h_id=choice(heroid)
        kill=choice(kills)
        death=choice(deaths)
        assist=choice(assists)
        #goldd=choice(gold)
        goldd=randint(1000,45000)
        levell=choice(levels)
        iwin=winstat[w]
        #assemble one record
        o_reco+=[M_id,j,h_id,kill,death,assist,goldd,levell,iwin]
        w+=1
        sr_table.append(o_reco)

i=0
big_src=[]

for j in range(2000000):
    temp=[]
    temp=temp+sr_table[i]+sr_table[i+1]+sr_table[i+2]+sr_table[i+3]+sr_table[i+4]
    big_src.append(temp)
    i+=5

with open('position_origin.txt', 'w') as f:
    for item in big_src:
        f.write("%s\n" % item)
