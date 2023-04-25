import requests
import json
import time
import os
from tqdm import tqdm
import pandas as pd
# from crawl_posts import crawl_json
# from gen_seq import gen_sequences
from datetime import date

def read_urls_sw():
    today = date.today().strftime("%Y%m%d")
    sub = 'hongkong'
    json_path = f'./jsonpath/post/post_{sub}_{today}.json'
    #load json file and get urls
    with open(json_path, 'r') as f:
        alldata = json.load(f)

        start_urls = []
        for entry in alldata:
            start_urls.append('https://www.reddit.com' + entry['permalink'])
        return start_urls




def crawl_comments_sw(start_urls, hdr, sub, json_path):
    # num = []
    data_all = []
    today = date.today().strftime("%Y%m%d")
    for u in tqdm(start_urls):
        url = u + '.json'    
        req = requests.get(url, headers=hdr)
        json_data = json.loads(req.text)
        if (url == 'https://www.reddit.com/r/HongKong/comments/12r9jql/rhongkong_weekly_discussion/.json'):
            print(json_data)
        data_all.append(json_data)
        # num_comments = json_data[0]['data']['children'][0]['data']['num_comments']
        # num.append(num_comments)
        # print(num_comments)
        time.sleep(2)
    with open(os.path.join(json_path, 'comment/all_comments_of_{}_{}.json'.format(sub,today)), 'w') as f:
        json.dump(data_all, f, indent=4)
    print('{} of posts and comments crawled.'.format(len(data_all)))


def read_urls_sub(path_json, subreddit):
    """
    Collect URLs of crawled posts from JSON file without duplication
    :param path_json: the path of crawled JSON files
    :param subreddit: a list of subreddits
    :return: a list of URLs of crawled posts.
    """
    list_json = os.listdir(path_json)
    start_urls = []
    for json_name in list_json:
        if subreddit == json_name[4:-14]:
            with open(os.path.join(path_json, json_name), 'r') as f:
                data_all = json.load(f)
            for i in range(0, len(data_all)):
                if data_all[i]['data']['url'] not in start_urls:  # the URL is a unique ID
                    start_urls.append(data_all[i]['data']['url'])
    return start_urls


def crawl_comments_sub(start_urls, hdr, sub, json_path):
    """
    Crawl the comments of subreddit into JSON files with appending
    :param start_urls: lists of URLs of posts
    :param hdr: request header
    :param sub: name of subreddit
    :param json_path: path of the project
    :return: none, write JSON file
    """
    print("===================================")
    print('Crawling comments of {} ...'.format(sub))
    path_com = os.path.join(json_path, 'api_coms/sub_{}.json'.format(sub))
    if not os.path.isfile(path_com):
        data_all, urls_crawled = [], []
    else:
        with open(path_com, mode='r', encoding='utf-8') as fr:
            data_all = json.load(fr)
            urls_crawled = []  # load all urls crawled
            for data in data_all:
                url = data[0]['data']['children'][0]['data']['url']
                urls_crawled.append(url)
    for u in tqdm(start_urls):
        if u not in urls_crawled and 'https://www.reddit.com/r/' in u:
            url = u + '.json'
            req = requests.get(url, headers=hdr)
            json_data = json.loads(req.text)
            data_all.append(json_data)
            time.sleep(2)
    with open(path_com, mode='w', encoding='utf-8') as fw:
        json.dump(data_all, fw)
    print('{} of posts and comments crawled.'.format(len(data_all)))


if __name__ == '__main__':
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.0.0',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cache-Control': 'max-age=0'
        }

    path_json = "./jsonpath"

    df_sub = pd.read_csv(os.path.join('./subreddits_list', 'subreddits_top.csv'))
    subreddits = df_sub['subreddit'].values

    # crawl_json(list_subreddit=subreddits, hdr=header)

    stat_info = []
    # for sub in subreddits:
    #     p = os.path.join(path_json, 'api_json/')
    #     urls = read_urls_sub(path_json=p, subreddit=sub)
    #     crawl_comments_sub(start_urls=urls, hdr=header, sub=sub, json_path=path_json)
    #     stat_subreddit = gen_sequences(path_json, sub)
    #     stat_info.append(stat_subreddit)
    
    for sub in subreddits:
        urls = read_urls_sw()
        crawl_comments_sw(start_urls=urls, hdr=header, sub=sub, json_path=path_json)
        # stat_subreddit = gen_sequences(path_json, sub)
        # stat_info.append(stat_subreddit)


