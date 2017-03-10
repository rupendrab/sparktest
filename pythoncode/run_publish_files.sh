#!/bin/bash

cd $(dirname $0)

./publish_files.py \
    --host localhost:9092 \
    --topic books \
    --input /data/books/streaming/ \
    --files '*.txt' \
    --staging /data/books/streaming/staging/ \
    --processed /data/books/streaming/done/

