from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections
spark = SparkSession.builder.config("spark://ec2-3-90-122-232.compute-1.amazonaws.com:7077").appName("groupplayerhistory").getOrCreate()


def match_mapper(line):
    fields = line.split(',')
    return Row(
MID1=int(fields[0][:-1]), AID1=int(fields[1]), SIDE1=str(fields[2]), WIN1=str(fields[3]),
MID2=int(fields[4][:-1]), AID2=int(fields[5]), SIDE2=str(fields[6]), WIN2=str(fields[7]),
MID3=int(fields[8][:-1]), AID3=int(fields[9]), SIDE3=str(fields[10]), WIN3=str(fields[11]),
MID4=int(fields[12][:-1]), AID4=int(fields[13]), SIDE4=str(fields[14]), WIN4=str(fields[15]),
MID5=int(fields[16][:-1]), AID5=int(fields[17]), SIDE5=str(fields[18]), WIN5=str(fields[19]),
MID6=int(fields[20][:-1]), AID6=int(fields[21]), SIDE6=str(fields[22]), WIN6=str(fields[23]),
MID7=int(fields[24][:-1]), AID7=int(fields[25]), SIDE7=str(fields[26]), WIN7=str(fields[27]),
MID8=int(fields[28][:-1]), AID8=int(fields[29]), SIDE8=str(fields[30]), WIN8=str(fields[31]),
MID9=int(fields[32][:-1]), AID9=int(fields[33]), SIDE9=str(fields[34]), WIN9=str(fields[35]),
MID0=int(fields[36][:-1]), AID0=int(fields[37]), SIDE0=str(fields[38]), WIN0=str(fields[39]))


def players_mapper(record):
    rec=record.split(',')
    return Row(
    GID=int(rec[0]),ACID=int(rec[1]),kills=int(rec[10]),deaths=int(rec[11]),assists=int(rec[12]),\
    gold_per_min=int(rec[17]), xp_per_min=int(rec[18]))

#collcet matches

lines = spark.sparkContext.textFile("raw_match.csv")
matches = lines.map(match_mapper)
schemamatch = spark.createDataFrame(matches).cache()
schemamatch.createOrReplaceTempView("matches")

#collect playerss
players=spark.sparkContext.textFile("raw_players.csv").map(players_mapper)
players=spark.createDataFrame(players).cache()

matches = spark.sql("SELECT MID1, AID1, SIDE1, WIN1 FROM matches union\
SELECT MID2, AID2, SIDE2, WIN2 FROM matches union\
SELECT MID3, AID3, SIDE3, WIN3 FROM matches union\
SELECT MID4, AID4, SIDE4, WIN4 FROM matches union\
SELECT MID5, AID5, SIDE5, WIN5 FROM matches union\
SELECT MID6, AID6, SIDE6, WIN6 FROM matches union\
SELECT MID7, AID7, SIDE7, WIN7 FROM matches union\
SELECT MID8, AID8, SIDE8, WIN8 FROM matches union\
SELECT MID9, AID9, SIDE9, WIN9 FROM matches union\
SELECT MID0, AID0, SIDE0, WIN0 FROM matches union")

# | MID | AID | SIDE | WIN |
matches=spark.createDataFrame(matches).cache()

#collect playerss
# | MID | AID | SIDE | WIN | kills | deaths | assists | gold_per_min | xp_per_min |
joined=spark.sql("SELECT * FROM matches JOIN players ON MID1 = GID AND AID1=ACID")

filtered_joined = spark.sql(
"SELECT * FROM joined WHERE MID IN \
(SELECT MID FROM (SELECT MID, count(MID) FROM joined GROUP BY MID,SIDE HAVING COUNT(MID) = 5) AS a) ORDER by MID, AID")

results=matches.collect()

with open("reformed_matches.csv","w") as newfile:
    csv_writer=csv.writer(newfile, delimiter=",")
    for line in results:
        csv_writer.writerow(line)
