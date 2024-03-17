#!/usr/bin/bash


while true; do

    echo "crawling posts"
    python crawl_posts.py

    echo "crawling comments"
    python comments.py

    echo "generating seq_json_file && export to mysql"
    python gen_seq.py 

    echo "Current time: $(date)"

    echo "Waiting for 24 hours for next crawling"

    sleep 86400

done

