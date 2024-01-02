# RedditAPISpider

Reddit爬虫

## 批量爬取使用方法
在主目录下运行automatic.py
```
python automatic.py
```
会得到一个“run.sh”脚本文件，使用screen打开后台终端
```
screen -U -S reddit
```
由于是海外平台，需要使用代理来使用API，确保在本地搭建好代理之后使用以下指令来导入代理（端口是你搭建的代理监听的端口）
```
export ALL_PROXY=socks5://127.0.0.1:10086
export http_proxy=http://127.0.0.1:10087
export https_proxy=http://127.0.0.1:10087
```
运行脚本（脚本运行前确保激活虚拟环境“reddit"和切换到工作目录）
```
conda activate reddit && cd ./RedditAPISpider && bash run.sh
```
记得不定期检查/RedditAPISpider/jsonpath/seq目录下的文件是否会过大，若文件过大建议删除