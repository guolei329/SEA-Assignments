#!/usr/bin/env python
import sys, json, preprocess
from collections import defaultdict

if __name__ == "__main__":
    index = defaultdict(list)
    for doc_id, title, text, tf in preprocess.doc_iterator(sys.stdin):
        for term in tf:
            index[term].append((doc_id, tf[term]))
    for term in index:
        print("%s\t%s" % (term, json.dumps(index[term])))

