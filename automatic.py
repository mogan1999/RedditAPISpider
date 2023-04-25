script = """#!/bin/bash

source $(conda info --base)/etc/profile.d/conda.sh
conda activate reddit

while true; do
    python crawl_post.py
    python comments.py
    python gen_seq.py
    echo "Current time: $(date)"
    echo "Waiting for 24 hours for next crawling"
    sleep 86400
done
"""

with open("run_scripts.sh", "w") as f:
    f.write(script)
