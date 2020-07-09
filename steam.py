]from pyspark.sql.functions import date_trunc
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, count, hour
'''
Description: 
PySpark script to aggregate 
and inspect steam data

1. Load .csv for Player_Summaries, Game_  into PySpark dataframes.
2. Join all `Games_` tables into one dataframe.
3. Count the number of games per `publisher` and per `genre`.
4. Find day and hour when most new accounts were created 

Date:23/03/2020
Last updated:24/03/2020
Author:Krishna Mooroogen

'''
# Part 1 
#definition of filenames of game files
names=['Games_1.csv','Games_2.csv','Games_Developers.csv','Games_Genres.csv','Games_Publishers.csv']
#load/store CSVs in dict as dataframes by name using list comprehension 
d = {name:spark.read.format("csv").option("header", "true").load('Documents/steam_gaming_small/'+name) for name in names}

#Part 2 
#append Game_1 + Game_2 
games=d[names[0]].union(d[names[1]])
#Join Game_ dataframes on appid 
game_df=games.join(d[names[2]],on=['appid'],how='left').join(d[names[3]],on=['appid'],how='left').join(d[names[4]],on=['appid'],how='left')

#Part 3
#remove nulls
game_df = game_df.filter(game_df.Publisher.isNotNull())
#count/Group by Developer and Genre
results=game_df.groupby('Publisher','Genre').count().sort('Publisher')
#blank publisher is from indie dev 

#output results into csv
results.coalesce(1).write.csv("Documents/pub_gen.csv",header=True)

#Part 4
#load Player_summeries
player_sum = spark.read.format("csv").option("header", "true").load("Documents/steam_gaming_small/Player_*.csv")
#remove nulls in timecreated column 
player_sum=player_sum.filter(player_sum.timecreated.isNotNull())
#cast column timecreated to timestamp and trunc by hour
player_sum=player_sum.withColumn("hour", date_trunc("hour", col("timecreated").cast("timestamp")))
#count number of new accounts made and sort descending 
hours=player_sum.groupby('hour').count().sort(desc('count'))
#find max value in count (where most accounts were made)
mx=a.agg({"count":"max"}).collect()[0][0]
#select hour at max count for outputting to csv
results=hours.select("hour").where("count=="+str(mx))
results.coalesce(1).write.csv("Documents/hour.csv",header=True)
