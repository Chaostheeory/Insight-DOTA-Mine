# DOTA-Mine
Insight Data Engineering Fellow Project
# Slides
[slides](https://drive.google.com/open?id=1_rvLLf2qUtAJEqjw0pODiOaDBquZnzTkT8UcnqmVomw)

# About/Motivation
DOTA Mine is an application that correlates extract features from raw gaming data for players and analysts.

[Dota2](https://en.wikipedia.org/wiki/Dota_2) is a multiplayer online battle arena (MOBA) video game developed and published by Valve Corporation, it has a widespread and active esports scene. However despite its popularity, it receives criticism for its steep learning curve and complexity. 
Dota2 provides players conprehensive end game stats but does not include [position](https://imperium.news/dota-2-guide-competitive-positions/). However position is very important tag that defines a player. Thus, the objective of DOTA Mine is to relate players' position with their team performance. 
Five stats were used for extraction of position: Kills, Deaths, Assists, Gold, Levels. They each were assigned to a weight for summation. The position is based on the ranking of summation within the team.

# Pipeline
![al text](https://github.com/Chaostheeory/DOTA-Mine/blob/master/image/Picture1.png)
The data was found at [The OpenDota Blog](https://blog.opendota.com/2017/03/24/datadump2/). The sample data (under 10G) can be downloaded directly. However the two datasets are not correlated, so the correlations was simulated by changing the player IDs.
The calculation was done in spark because all the extraction can be done for each game independently. 
# Future application
At the time the pipeline only correlates position with over all winrate. In the future position can be correlates with heroes as well potentially [for hero draft](https://firstblood.io/pages/blog/dota-2/how-to-draft-a-balanced-lineup-in-dota-2/) analysis, as drafting is a another essential part of Dota.

