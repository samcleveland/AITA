import os
os.chdir('C:\\Users\\samcl\\Documents\\2023\\Project\\AITA')

from RedditData import RedditData
from SQL import SQL
import time

passcode = {}
postgres = {}

f = open('C:/Users/samcl/Documents/2023/Project/AITA/key.txt')
for line in f.readlines():
    line = line.strip().split(":")
    passcode[line[0]] = line[1]
f.close()

f = open('C:/Users/samcl/Documents/2023/Project/AITA/postgres.txt')
for line in f.readlines():
    line = line.strip().split("=")
    postgres[line[0]] = line[1]
f.close()


rd = RedditData(passcode['Client_ID'], 
                passcode['Secret_token'], 
                passcode['UserName'], 
                passcode['password'])

post, comment = rd.posts('AmITheAsshole','hot')


sql = SQL('AITA', postgres['user'], 'localhost', postgres['password'])

#sql.drop_tables()
sql.insert_post(post)
sql.insert_comment(comment)
sql.close()