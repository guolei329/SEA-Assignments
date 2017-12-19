#!/usr/bin/env python
import sys, json, pickle
from collections import defaultdict

if __name__ == "__main__":
    index = defaultdict(list)
    for line in sys.stdin:
        doc_id, json_dump = line.strip().split("\t")
        posting_list = json.loads(json_dump)
        for (term, tf) in posting_list:
            index[term].append((int(doc_id), tf))

    pickle.dump(index, sys.stdout.buffer)
