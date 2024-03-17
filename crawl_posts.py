#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python version: 3.6
import argparse
import json
import os
import time
from datetime import date, datetime
from pprint import pprint

import mysql.connector
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python version: 3.6


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
        cursor.execute("SELECT id, num_comments FROM posts WHERE id = %s", (entry['id'],))
        row = cursor.fetchone()

        if row is not None:
            # Update num_comments value if id exists and num_comments are different
            if int(row[1]) != entry['num_comments']:
                cursor.execute("UPDATE posts SET num_comments = %s WHERE id = %s",
                          (entry['num_comments'], entry['id']))
        else:
            # Insert new row if id does not exist
            cursor.execute("INSERT INTO posts (id, subreddit, permalink, title, selftext, created, num_comments, author) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                      (entry['id'], entry['subreddit'], entry['permalink'], entry['title'], entry['selftext'],
                       created_str, entry['num_comments'], entry['author']))
        cnx.commit()
    except mysql.connector.Error as e:
        print('Error:', e.args[0])
    print('Records created.')
    return 0


def get_reddit_token(client_id, client_secret):
    """
    获取Reddit的OAuth2访问令牌。
    """
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {'grant_type': 'client_credentials'}
    headers = {'User-Agent': 'YourApp/0.1 by mogan1999'}
    response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
    
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print("获取访问令牌失败，状态码：", response.status_code)
        return None


def crawl_json(list_subreddit, hdr):
    """
    Crawl each pages of a list of subreddit in specific day.
    :param list_subreddit: list of subreddits
    :param hdr: request header
    :return:
    """

    limit = 100  # Number of posts 
    timeframe = 'day'  # hour, day, week, month, year, all
    listing = 'hot'  # controversial, best, hot, new, random, rising, top
    for sub in tqdm(list_subreddit):
        # Initialize data_all at the beginning of each subreddit processing
        data_all = []  # Ensure data_all is defined outside the try block
        
        # Database connection setup
        cnx = mysql.connector.connect(user='root', password='123456',
                                      host='localhost', database=sub + 'data')
        cursor = cnx.cursor()
        print('Opened database successfully.')
        
        # Ensure the posts table exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS posts
                        (id VARCHAR(20) PRIMARY KEY, subreddit TEXT, permalink TEXT, title TEXT, selftext TEXT, created TEXT, num_comments TEXT, author TEXT)''')
        
        print(f'Crawling subreddit {sub}')
        url = f'https://oauth.reddit.com/r/{sub}/{listing}.json?limit={limit}&t={timeframe}'

        # Perform the request
        req = requests.get(url, headers=hdr)
        if req.status_code == 200:  # Check if request was successful
            try:
                json_data = json.loads(req.text)
                entries = json_data['data']['children']
                for entry in entries:
                    current_num_comments = entry['data']['num_comments']

                    # Check if post exists in the database and if the number of comments has changed
                    cursor.execute("SELECT num_comments FROM posts WHERE id = %s", (entry['data']['id'],))
                    row = cursor.fetchone()

                    if row is None or (row is not None and int(row[0]) != current_num_comments):
                        data_all.append(entry['data'])
                        pprint(entry['data']['title'])

                        # Process the post data
                        process_data(entry['data'], cursor, cnx)

            except json.JSONDecodeError as e:
                print(f'Error parsing JSON: {e}')
        else:
            print(f"Failed to get data, status code: {req.status_code}")

        # Save the collected data into a JSON file
        today = date.today().strftime("%Y%m%d")
        with open(f'./jsonpath/post/post_{sub}_{today}.json', 'w') as f:
            json.dump(data_all, f, indent=4)
            print('json_today saved.')
        print(f'{len(data_all)} posts crawled.')

    # Ensure to close database connection
    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    client_id = 'c8w6xFd494SnadOJg4XWRw'
    client_secret = 'caK60Zh2CBzawcYGGxgL7p-xmr5iyw' 
    access_token = get_reddit_token(client_id, client_secret)
        # 获取访问令牌
    print(access_token)
    if access_token is None:
        print("未获取到访问令牌，无法进行爬取。")
        exit(1)
    header = {
        'Authorization': f'bearer {access_token}',
        'User-Agent': 'MyRedditApp/1.0 by mogan1999'}

    path_project = "./subreddits_list/"

    # crawl icd subreddits
    # df_icd = pd.read_csv(os.path.join(path_project, 'subreddits_icd.csv'))
    # subreddits = df_icd['subreddit'].values
    # crawl_json(subreddits, header)

    # crawl top subreddits
    df_top = pd.read_csv(os.path.join(path_project, 'subreddits_top.csv'))
    tops = df_top['subreddit'].values
    crawl_json(tops, header)


