from xml.dom import minidom
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--num_partitions", type=int, required=True)
    parser.add_argument("--job_path", required=True)
    args = parser.parse_args()

    xmldoc = minidom.parse(args.filename)
    doc_list = xmldoc.getElementsByTagName('page')
    chunk_size = len(doc_list) // args.num_partitions + 1

    output = None
    for doc_id in range(len(doc_list)):
        if doc_id % chunk_size == 0:
            output = open(args.job_path + "/" + str(int(doc_id / chunk_size)) + ".in", "w")
        page = doc_list[doc_id]
        title = page.getElementsByTagName('title')[0].childNodes[0].nodeValue
        text = page.getElementsByTagName('text')[0].childNodes[0].nodeValue
        text = text.replace("\n", " ")
        output.write("%d\t%s\t%s\n" % (doc_id, title, text))
    output.close()
