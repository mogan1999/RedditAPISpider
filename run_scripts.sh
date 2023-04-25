#!/usr/bin/bash



conda activate reddit



while true; do

    python crawl_posts.py

    python comments.py

    python gen_seq.py

    echo "Current time: $(date)"

    echo "Waiting for 24 hours for next crawling"

    sleep 86400

done

