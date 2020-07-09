import pandas as pd 
import numpy as np 
import seaborn as sb 
import matplotlib.pyplot as plt 

'''

Description: 
Python script with game addiction 
quick insights using pandas and seaborn 
based on three surface ideas for exploration

1: show the total game time of players since time of making account as a percentage usng only Game_2
2: total game time per genre
3: looking at long term consistent play based on 2week values from Game_1 to Game_2 

Date:27/02/20
Last updated: 29/03/20
Author: Krishna Mooroogen

'''

#Reading in data to dataframe 

names=['Games_1.csv','Games_2.csv','Games_Developers.csv','Games_Genres.csv','Games_Publishers.csv','Player_Summaries.csv']
d = {name:pd.read_csv(name) for name in names}


#idea 1 explore total game time of players from time they created account

'''
#check if steamids' from g1 are diff in g1 as g2 as there is overlap
#we only want the playtime of unique players

g1_unique=d[names[0]].steamid.unique()
g2_unique=d[names[1]].steamid.unique()

if np.setdiff1d(g1_unique, g2_unique) > 0: 
	vals = np.setdiff1d(g1_unique, g2_unique)
'''

#assigned play_sum dataframe
play_sum=d[names[5]]    
#subdataframe 
play_sum=play_sum[['steamid','timecreated']]
#Join player summaries to game_2 
game_sum=pd.merge(d[names[1]],play_sum,how='left',on='steamid')
#copy protect
game_sum=game_sum.copy()
#recast datetime #assume UTC for both for ease

game_sum['dateretrieved']=pd.to_datetime(game_sum['dateretrieved'])
game_sum['dateretrieved'].dt.tz_localize(None)
game_sum['timecreated']=pd.to_datetime(game_sum['timecreated'])          

#create time delta field, time between account created and data retrieved 

game_sum['td']=((game_sum.dateretrieved-game_sum.timecreated).dt.days*24*60)+(game_sum.dateretrieved-game_sum.timecreated).dt.seconds/60     

#create dataframe with playtime summed  - some redundency here 
tot_play_times=game_sum.groupby('steamid').sum()
#rejoin dataframes to include new field 
tot_time_steamid=pd.merge(tot_play_times_g2,game_sum[['steamid','td']],how='left',on='steamid')
#create percent field
tot_time_steamid['time_perc']=(tot_time_steamid.playtime_forever/tot_time_steamid.td_y)*100

tot_time_steamid.drop_duplicates(subset ="steamid",keep = True, inplace = True)

#need consider time that players started (if created account yesterday did they just play a lot in that day)
#checked distribution found no entries with small time delta
#weighting should be used in reality - i.e. based on the time delta, but also non gaming apps could be removed 
#could also weight by genre because some games just havelarger content 
#drop NaNS s

tot_time_steamid=tot_time_steamid.dropna()   

#number of players spending more than 5% of total time gaming since creating account
#again this shoudl reflect time window 
print ('Number of players sepnding more than 5% of time gaming',len(tot_time_steamid[tot_time_steamid['time_perc']>5]))

tot_time_steamid_5perc=tot_time_steamid[tot_time_steamid['time_perc']>5] 
#plot histgram of highest played players
sb.distplot(tot_time_steamid_5perc['time_perc'].values,kde=True,rug=False)                                           
plt.xlabel('Percentage of play time > 5%')   

#idea 2
#playtimes by most played genre
#drop redundent field and join genre data
game_sum=game_sum.sort_values('steamid').reset_index(drop=True)
g_genre=d[names[3]]
game_sum_g=pd.merge(game_sum,g_genre,how='left',on='appid')
game_sum_g=game_sum_g.copy()
game_sum_g=game_sum_g.drop(columns=['playtime_2weeks'])

#id_td=game_sum_g[['steamid','td']]    # easier

#some redundency here, doesnt take into account app having multiple genre tags
#playtime by genre
game_sum_genre['playtime_genre']=game_sum_g2.groupby(['steamid','Genre'])['playtime_forever'].transform('sum)
gs=game_sum_genre[['steamid','td','Genre','playtime_genre']] 
#find per player which genre was played most (max)       
idx = gs.groupby(['steamid'])['playtime_genre'].transform(max) == gs['playtime_genre']
gs=gs[idx].drop_duplicates('steamid',keep='first')

#count only high values - removes non gaming titles 
cnts = gs['Genre'].value_counts()
gs_new=gs[gs.isin(cnts.index[cnts >= 400]).values]
#plot
sb.countplot(gs_new['Genre'])

#idea 3
#looking at consistent play based on the two week periods 

#define game_1 and _2 dataframes
g1=d[names[0]]
g2=d[names[1]]

#subset
g2_test=g2[['steamid','appid','playtime_2weeks_followup']]

#to find consistent play of same app join only on same player and same apps

g3=pd.merge(g1,g2_test,how='left', left_on=['steamid','appid'],right_on=['steamid','appid'])
g3=g3.copy()
g3=g3.dropna()
g4=g3.drop(columns=['appid','playtime_forever','dateretrieved'])

#find delta between twoweek play periods - small deltas indicate consitent play times
g4['dt_2weeks']=abs(g4['playtime_2weeks']-g4['playtime_2weeks_followup'])

#subset of where play time is longer than 3000 mins
g_longterm=g4[g4['playtime_2weeks']>3000]   

#in two weeks make subset where delta is smaller that 5 hours assuming that's a reasonable deviation 
consistent_long=g_longterm[g_longterm['dt_2weeks']<300]

sb.distplot(g_longterm['playtime_2weeks'],kde=False,label='playtime 2 weeks')    
sb.distplot(g_longterm['playtime_2weeks_followup'],kde=False,label='playtime 2 weeks follow up')    
plt.xlabel('playtime minutes')

#based on sample possible to track 
print('percent of long term players',len(consistent_long)/len(g_longterm)*100)









