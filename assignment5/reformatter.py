# -*- coding: utf-8 -*-

import mwxml, argparse, string

UGLY_TEXT_MAP = dict([(ord(char), ' ') for char in string.punctuation + "“”‘’–—"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--num_partitions", type=int, required=True)
    parser.add_argument("--job_path", required=True)
    args = parser.parse_args()

    num_pages = sum(1 for _ in mwxml.Dump.from_file(args.filename).pages)

    dump = mwxml.Dump.from_file(args.filename)
    chunk_size = num_pages // args.num_partitions + 1

    output = None
    for doc_id, page in enumerate(dump.pages):
        if doc_id % chunk_size == 0:
            output = open(args.job_path + "/" + str(int(doc_id / chunk_size)) + ".in", "w")
        if page.namespace == 14:
            continue
        for revision in page:
            text = revision.text
        text = ' '.join(text.translate(UGLY_TEXT_MAP).replace('\n', ' ').lower().split())
        output.write("%s\n" % text)
    output.close()
