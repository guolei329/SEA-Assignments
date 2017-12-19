#!/usr/bin/env python
import sys, json, math, pickle
from collections import defaultdict

if __name__ == "__main__":
    df = defaultdict(int)
    docs = set()
    for line in sys.stdin:
        term, json_dump = line.strip().split("\t")
        posting_list = json.loads(json_dump)
        df[term] += len(posting_list)
        docs.update([posting[0] for posting in posting_list])

    log_idf = {term: math.log(len(docs) / float(df[term])) for term in df}

    pickle.dump(log_idf, sys.stdout.buffer)
