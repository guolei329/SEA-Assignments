#!/bin/bash


wget -c -O assignment2/data/info_ret.zip http://cs.nyu.edu/courses/spring17/CSCI-GA.3033-006/assignment2_files/info_ret.zip
unzip -o -d assignment2/data/ assignment2/data/info_ret.zip
python -m assignment4.reformatter assignment2/data/info_ret.xml --job_path=assignment4/idf_jobs/ --num_partitions=5
python -m assignment4.reformatter assignment2/data/info_ret.xml --job_path=assignment4/docs_jobs/ --num_partitions=5
python -m assignment4.reformatter assignment2/data/info_ret.xml --job_path=assignment4/invindex_jobs/ --num_partitions=5

