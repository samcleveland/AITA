import psycopg2
from datetime import datetime
import pandas as pd

class SQL:
    def __init__(self, dbname, user, host, password):
        self.conn = self.connect(dbname, user, host, password)
        self.cur = self.conn.cursor()
        
    def connect(self, dbname, user, host, password):
        try:
            conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password}")
            print(f'Connected to database at {datetime.now()}')
            return conn
        except:
            print("Unable to connect to the database")
            
    def close(self):
        self.conn.commit()
        self.conn.close()
        print(f'Closed connection to database at {datetime.now()}')
            
    def create_post_query(self):
        return '''CREATE TABLE IF NOT EXISTS post (
        post_index VARCHAR(8),
        username VARCHAR(32),
        title VARCHAR(300),
        selftext VARCHAR(16000),
        flair VARCHAR(32),
        url VARCHAR(160),
        edited timestamp without time zone,
        timestamp timestamp without time zone,
           
        PRIMARY KEY (post_index));'''
        
    def create_comment_query(self):
        return '''CREATE TABLE IF NOT EXISTS comment(
        comment_index VARCHAR(8),
        username VARCHAR(32),
        text VARCHAR(8192),
        score INTEGER,
        controversaility NUMERIC(4, 3),
        edited timestamp,
        timestamp timestamp,
        post_link VARCHAR(8),
        early_indicator VARCHAR(5),
    
        PRIMARY KEY (comment_index),
        
        FOREIGN KEY (post_link)
            REFERENCES post(post_index));'''
            
            
    def drop_tables(self):
        self.cur.execute('''DROP TABLE IF EXISTS comment;
        DROP TABLE IF EXISTS post;''')
        
        self.conn.commit()
        
    def insert_post(self, df):
        self.cur.execute(self.create_post_query())
        for i, r in df.iterrows():
            self.cur.execute('''INSERT INTO post VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (post_index) DO UPDATE
                             SET flair = EXCLUDED.flair, selftext = EXCLUDED.selftext, edited = EXCLUDED.edited;''', [i, *r])
            
        self.conn.commit()
            
    def insert_comment(self, df):
        self.cur.execute(self.create_comment_query())
        for i, r in df.iterrows():
            self.cur.execute('''INSERT INTO comment VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (comment_index) DO UPDATE
                             SET text = EXCLUDED.text, score = EXCLUDED.score, edited = EXCLUDED.edited, early_indicator = EXCLUDED.early_indicator;''', [i, *r])
            
        self.conn.commit()
        
    def select_all(self, table):
        self.cur.execute(f'SELECT * FROM {table};')
        return self.output_df(self.cur.fetchall())
    
    def output_df(self, results):
        return pd.DataFrame.from_records(results)