#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import datetime
import time
import os
import csv
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import date, datetime
import mysql.connector
def create_comments_table(subreddit):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database=subreddit + 'data'
    )

    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            post_id VARCHAR(255),
            title VARCHAR(1024),
            created DATETIME,
            user VARCHAR(255),
            body TEXT,
            ups INT,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        )
    """)

    connection.commit()
    connection.close()

def insert_comments_to_db(comments_data, subreddit):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database=subreddit + 'data'
    )

    cursor = connection.cursor()
    for data in comments_data:
            post_id, title, created, user, body, ups = data

            # Check if the comment already exists in the database
            query = f"SELECT * FROM comments WHERE post_id = %s AND title = %s AND user = %s AND body = %s"
            cursor.execute(query, (post_id, title, user, body))
            result = cursor.fetchone()

            if result:
                # If the comment exists and its 'ups' value has changed, update the 'ups' value in the database
                if result[-1] != ups:  # Assuming 'ups' is the last column in your 'comments' table
                    query = f"UPDATE comments SET ups = %s WHERE post_id = %s AND title = %s AND user = %s AND body = %s"
                    cursor.execute(query, (ups, post_id, title, user, body))
                    connection.commit()
            else:
                # If the comment doesn't exist, insert it into the database
                query = """INSERT INTO comments (post_id, title, created, user, body, ups)
                        VALUES (%s, %s, %s, %s, %s, %s)"""
                cursor.execute(query, (post_id, title, created, user, body, ups))
                connection.commit()
    
    cursor.close()


def gen_sequences(path_base, subreddit):
    """
    Generate posts and comments sequences of each subreddit
    :param path_base: the path of the project
    :param subreddit: the name of subreddit
    :return: None
    """
    # def recursive_replies(comment_list):
    #     for c in comment_list:
    #         list_posts.append()
    #     return
    
    def json_serial(obj):
        """JSON serializer for objects not serializable by default."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    print("===================================")
    print('Generating sequences of {} ...'.format(subreddit))
    today = date.today().strftime("%Y%m%d")

    #查文件是否存在并加载现有内容
    seq_file = os.path.join(path_base, 'seq/seq_sub_{}.json'.format(subreddit))
    if os.path.isfile(seq_file):
        with open(seq_file, 'r') as fr:
            seq_all = json.load(fr)
    else:
        seq_all = []

    with open(os.path.join(path_base, 'comment/all_comments_of_{}_{}.json'.format(subreddit,today)), 'r') as fr:
        list_data = json.load(fr)
    # path_seq = '/data/shji/myprojects/redditscrapy/api_seq/sub_{}.json'.format(subreddit)
    # if not os.path.isfile(path_seq):
    #     seq_all, seq_crawled = [], []
    # else:
    #     with open(path_seq, mode='r', encoding='utf-8') as fr:
    #         seq_all = json.load(fr)

    new_seq_data = {} #存储新的序列
    list_com_count = []
    comments_data = []
    for item in tqdm(list_data):
        # print(ix)
        if len(item) >= 2:
            list_posts = []
            post_0 = item[0]
            child_0 = post_0['data']['children'][0]['data']
            author = child_0['author']
            list_posts.append((datetime.fromtimestamp(child_0['created']), child_0['author'], child_0['selftext']))
            coms = item[1]
            for item_child in coms['data']['children']:
                if item_child['kind'] != 'more':
                    comment = item_child['data']
                    list_posts.append((datetime.fromtimestamp(child_0['created']), comment['author'], comment['body'], comment['ups']))
                    comments_data.append((child_0['id'], child_0['title'], datetime.fromtimestamp(comment['created']), comment['author'], comment['body'], comment['ups']))
                    # print(comment.keys())
                    # print(comment['depth'])
                    if not comment['replies'] == '':
                        replies = comment['replies']['data']['children']
                        for re in replies:
                            comment_child = re['data']
                            if not 'body' in comment_child.keys():
                                continue
                            list_posts.append((datetime.fromtimestamp(comment_child['created']), comment_child['author'],
                                               comment_child['body'], comment_child['ups']))
                            comments_data.append((child_0['id'], child_0['title'], datetime.fromtimestamp(comment_child['created']), comment_child['author'], comment_child['body'], comment_child['ups']))
                            # print((datetime.fromtimestamp(comment_child['created']), comment_child['author'], comment_child['body'], comment_child['ups']))
                            if not comment_child['replies'] == '':
                                replies_child = comment_child['replies']['data']['children']
                                for re_child in replies_child:
                                    comment_grandchild = re_child['data']
                                    if not 'body' in comment_grandchild.keys():
                                        continue
                                    list_posts.append((datetime.fromtimestamp(comment_grandchild['created']), comment_grandchild['author'],
                                                       comment_grandchild['body'], comment_grandchild['ups']))
                                    comments_data.append((child_0['id'], child_0['title'], datetime.fromtimestamp(comment_grandchild['created']), comment_grandchild['author'], comment_grandchild['body'], comment_grandchild['ups']))
                else:
                    with open(os.path.join(path_base, 'more_urls.txt'), 'a') as fw:
                        fw.write(child_0['url'])
            # post_length = len(list_posts)
            seq_data = {'subreddit': subreddit,
                        'post_id': child_0['id'],
                        'author': author,
                        'title': child_0['title'],
                        'created_time': datetime.fromtimestamp(child_0['created']),
                        'selftext': child_0['selftext'],
                        'list_posts': list_posts}
            post_id = child_0['id']
            new_seq_data[post_id] = seq_data
        list_com_count.append(len(list_posts))
    
    
    for post_id, new_data in new_seq_data.items():
        existing_data = next((item for item in seq_all if item['post_id'] == post_id), None)
        if existing_data:
            # Update the existing data with new comments
            existing_data['list_posts'].extend(new_data['list_posts'])
        else:
            # Add new post to seq_all
            seq_all.append(new_data)

    with open(seq_file, 'w') as fw:
        json.dump(seq_all, fw, indent=4, default=json_serial)

    print('{} of sequences generated.'.format(len(seq_all)))

    # Insert comments to MySQL
    create_comments_table(subreddit)
    insert_comments_to_db(comments_data, subreddit)

    data_dict = {'date': today, 'subreddit': subreddit,
                 'com_num': len(list_com_count), 'com_mean': np.mean(list_com_count),
                 'com_median': np.median(list_com_count), 'com_max': max(list_com_count)}
    header_needed = not os.path.isfile('./statistic/stat.csv')

    with open('./statistic/stat.csv', 'a') as f:
        field_names = ['date', 'subreddit', 'com_num', 'com_mean', 'com_median', 'com_max']
        writer = csv.DictWriter(f, fieldnames=field_names)
        if header_needed:
            writer.writeheader()
        writer.writerow(data_dict)

    return data_dict


if __name__ == '__main__':
    df_sub = pd.read_csv(os.path.join('./subreddits_list', 'subreddits_top.csv'))
    subreddits = df_sub['subreddit'].values
    path_base = "./jsonpath"

    for sub in subreddits:
        gen_sequences(path_base, sub)