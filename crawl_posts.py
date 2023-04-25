#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python version: 3.6

import requests
import json
import time
import os
from tqdm import tqdm
import pandas as pd
from pprint import pprint
from datetime import datetime, date
import mysql.connector

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python version: 3.6

import requests
import json
import time
import os
from tqdm import tqdm
import pandas as pd
from pprint import pprint
from datetime import datetime, date
import mysql.connector

def process_data(entry, cursor, cnx):
    """
    Process data and insert/update it into the database
    :param entry: a dict of post data
    :return: 0
    """
    try:
        # Convert Unix time to standard time
        created = datetime.fromtimestamp(entry['created'])
        created_str = created.strftime('%Y-%m-%d %H:%M:%S')

        # Check if the post with same id already exists in the database
        cursor.execute("SELECT id, num_comments FROM POSTS WHERE id = %s", (entry['id'],))
        row = cursor.fetchone()

        if row is not None:
            # Update num_comments value if id exists and num_comments are different
            if int(row[1]) != entry['num_comments']:
                cursor.execute("UPDATE POSTS SET num_comments = %s WHERE id = %s",
                          (entry['num_comments'], entry['id']))
        else:
            # Insert new row if id does not exist
            cursor.execute("INSERT INTO POSTS (id, subreddit, permalink, title, selftext, created, num_comments, author) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                      (entry['id'], entry['subreddit'], entry['permalink'], entry['title'], entry['selftext'],
                       created_str, entry['num_comments'], entry['author']))
        cnx.commit()
    except mysql.connector.Error as e:
        print('Error:', e.args[0])
    print('Records created.')
    return 0


def crawl_json(list_subreddit, hdr):
    """
    Crawl each pages of a list of subreddit in specific day.
    :param list_subreddit: list of subreddits
    :param hdr: request header
    :return:
    """
    # Update the connection details below to match your MySQL setup
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='localhost', database='hongkongdata')
    cursor = cnx.cursor()
    print('Opened database successfully. ')
    cursor.execute('''CREATE TABLE IF NOT EXISTS POSTS
                 (id VARCHAR(20) PRIMARY KEY, subreddit TEXT, permalink TEXT, title TEXT, selftext TEXT, created TEXT, num_comments TEXT, author TEXT)''')
    subreddit = 'hongkong'
    limit = 100 #number of posts 
    timeframe = 'day' #hour, day, week, month, year, all
    listing = 'hot' # controversial, best, hot, new, random, rising, top
    for sub in tqdm(list_subreddit):
        print('Crawling subreddit {}'.format(sub))
        url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'.format(sub)
        req = requests.get(url, headers=hdr)
        try:
            json_data = json.loads(req.text)
            data_all = []
            entries = json_data['data']['children']
            for entry in entries:
                current_num_comments = entry['data']['num_comments']

                # 查询数据库中存储的帖子评论数量
                cursor.execute("SELECT num_comments FROM POSTS WHERE id = %s", (entry['data']['id'],))
                row = cursor.fetchone()

                # 如果找到帖子并且评论数量有变化，将帖子添加到 data_all
                if row is None or (row is not None and int(row[0]) != current_num_comments):
                    data_all.append(entry['data'])
                    pprint(entry['data']['title'])

                    # Process the post data
                    process_data(entry['data'], cursor, cnx)

        except json.JSONDecodeError as e:
            print('Error:', e)

        today = date.today().strftime("%Y%m%d")
        with open('./jsonpath/post/post_{}_{}.json'.format(sub, today), 'w') as f:
             json.dump(data_all, f, indent=4)
             print('json_today saved.')
        print('{} posts crawled. '.format(len(data_all)))
    # Close the connection after processing all posts
    cnx.commit()
    cursor.close()
    cnx.close()

if __name__ == '__main__':
    header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5'}

    path_project = "./subreddits_list/"

    # crawl icd subreddits
    # df_icd = pd.read_csv(os.path.join(path_project, 'subreddits_icd.csv'))
    # subreddits = df_icd['subreddit'].values
    # crawl_json(subreddits, header)

    # crawl top subreddits
    df_top = pd.read_csv(os.path.join(path_project, 'subreddits_top.csv'))
    tops = df_top['subreddit'].values
    crawl_json(tops, header)


