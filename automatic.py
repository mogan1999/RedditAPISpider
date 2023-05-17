script = """#!/usr/bin/bash\n

while true; do\n
    echo "crawling posts"
    python crawl_posts.py\n
    echo "crawling comments"
    python comments.py\n
    echo "generating seq_json_file && export to mysql"
    python gen_seq.py\n
    echo "Current time: $(date)"\n
    echo "Waiting for 24 hours for next crawling"\n
    sleep 86400\n
done\n
"""

with open("run.sh", "w", newline='\n') as f:
    f.write(script)
