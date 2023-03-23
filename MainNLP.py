import os
os.chdir('C:\\Users\\samcl\\Documents\\2023\\Project\\AITA')

from SQL import SQL
from NLP import NLP
import re
from nltk.sentiment import SentimentIntensityAnalyzer

postgres = {}

f = open('C:/Users/samcl/Documents/2023/Project/AITA/postgres.txt')
for line in f.readlines():
    line = line.strip().split("=")
    postgres[line[0]] = line[1]
f.close()

sql = SQL('AITA', postgres['user'], 'localhost', postgres['password'])



output = sql.select_all('output_table')
output.rename(columns = {0:'post_index',
                         1:'username',
                         2:'title',
                         3:'selftext',
                         4:'url'}, inplace = True)
sql.close()

nlp = NLP()
sia = SentimentIntensityAnalyzer()
ii = nlp.invertedIndex(list(output['title']))

#add sentiment analysis to dataframe
output['title_sent'] = output.apply(lambda row: sia.polarity_scores(row['title']), axis = 1)
output['text_sent'] = output.apply(lambda row: sia.polarity_scores(row['selftext']), axis = 1)
output['text_compound'] = output.apply(lambda row: sia.polarity_scores(row['selftext'])['compound'], axis = 1)


#regex to get info in parentheses
#re.findall('\(([^)]+)\)', output['selftext'].iloc[6])
