#!/usr/bin/env python
import sys, json, pickle

if __name__ == "__main__":
    doc = {}
    for line in sys.stdin:
        doc_id, json_dump = line.strip().split("\t")
        doc[int(doc_id)] = json.loads(json_dump)

    pickle.dump(doc, sys.stdout.buffer)
