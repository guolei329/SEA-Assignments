#!/usr/bin/env python
import sys, json, preprocess

if __name__ == "__main__":
    for doc_id, title, text, tf in preprocess.doc_iterator(sys.stdin):
        print("%d\t%s" % (doc_id, json.dumps([(term, tf[term]) for term in tf])))
