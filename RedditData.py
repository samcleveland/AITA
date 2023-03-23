import requests
import pandas as pd
import time
import re
from datetime import datetime

class RedditData:
    def __init__(self, client_id, secret_token, username, password):
        self.secret_token = secret_token
        self.client_id = client_id
        self.username = username
        self.password = password
        self.authorize()

    def authorize(self):
        'Authorize API'
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.secret_token) #get authorization tokens
        data = {'grant_type': 'password',
                'username': self.username,
                'password': self.password} #set username/password for API calls
        headers = {'User-Agent': 'AITA_NLP/0.0.1'} #set header so reddit knows what's going on
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=data, headers=headers) #get access tokens
        TOKEN = res.json()['access_token']
        self.headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
        
    def posts(self, thread, sort, after = None):
        'Start to crawl through posts'
        if after == None:
            params = {'limit': 100}
        else:
            params = {'limit': 100,
                      'after': after}
        
        post_df = pd.DataFrame() 
        comment_df = pd.DataFrame()
        res = requests.get(f"https://oauth.reddit.com/r/{thread}/{sort}",
                           headers=self.headers, params = params)
        
        #self.res = res.json()['data']['children']
        
        for post in res.json()['data']['children']:
            # append relevant data to dataframe
            if post['data']['link_flair_text'] in ['Open Forum', 'META']:
                pass
            else:
                post_df = pd.concat([post_df,
                                     pd.DataFrame({
                                                'user': post['data']['author'],
                                                'title': post['data']['title'],
                                                'selftext': post['data']['selftext'],
                                                'flair': post['data']['link_flair_text'],
                                                'url': post['data']['url'],
                                                'edited': datetime.fromtimestamp(post['data']['edited']),
                                                'timestamp': pd.Timestamp(post['data']['created'], unit='s')
                                            }, index = [post['data']['id']])])
            
                comment_df = pd.concat([comment_df, 
                                    self.comments(post['data']['url'])])
            
        return post_df, comment_df
    
    def update_post(self, url):
        post_df = pd.DataFrame() 
        comment_df = pd.DataFrame()
        params = {'limit': 100}
        url = url.replace('www','oauth')
        res = requests.get(f"{url}",
                           headers=self.headers, params = params)
        
        self.res = res
        
        for post in res.json()[0]['data']['children']:
            # append relevant data to dataframe
            if post['data']['link_flair_text'] in ['Open Forum', 'META']:
                pass
            else:
                post_df = pd.concat([post_df,
                                     pd.DataFrame({
                                                'user': post['data']['author'],
                                                'title': post['data']['title'],
                                                'selftext': post['data']['selftext'],
                                                'flair': post['data']['link_flair_text'],
                                                'url': post['data']['url'],
                                                'edited': datetime.fromtimestamp(post['data']['edited']),
                                                'timestamp': pd.Timestamp(post['data']['created'], unit='s')
                                            }, index = [post['data']['id']])])
            
                if len(post['data']['url']) > 0:
                    comment_df = pd.concat([comment_df, 
                                        self.comments(post['data']['url'])])
                
            time.sleep(1)
            
        return post_df, comment_df
            
    def comments(self, url):
        'Get top comments from post'
        c_df = pd.DataFrame() #initialize dataframe 
        post_res = requests.get(url.replace('www','oauth'),
                           headers=self.headers, params = {'limit':50})
        
        for comment in post_res.json()[1]['data']['children']:
            # append relevant data to dataframe
            if comment['kind'] == 't1' and comment['data']['author'] != 'Judgement_Bot_AITA':
                
                #determine how the comment categorizes the original post
                if len(re.findall('NTA|ESH|YTA|NAH',comment['data']['body'])) == 0:
                    early_indicator = False
                else:
                    early_indicator = re.findall('NTA|ESH|YTA|NAH',comment['data']['body'])[0]
                
                link = comment['data']['link_id'].split('_')[1]
                
                c_df = pd.concat([c_df,
                                    pd.DataFrame({
                                        'user': comment['data']['author'],
                                        'text': comment['data']['body'],
                                        'score': comment['data']['score'],
                                        'controversaility': comment['data']['controversiality'],
                                        'edited': datetime.fromtimestamp(comment['data']['edited']),
                                        'timestamp': pd.Timestamp(comment['data']['created'],unit='s'),
                                        'post_link': link,
                                        'early_indicator':early_indicator
                                    }, index = [comment['data']['id']])])
                
        return c_df