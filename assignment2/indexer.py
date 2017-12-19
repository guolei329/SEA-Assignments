from xml.dom import minidom
import nltk, pickle, math, re, os, zipfile, logging
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from collections import defaultdict, Counter
from urllib.request import urlopen
import inventory

log = logging.getLogger(__name__)

TITLE_BONUS = 10

UGLY_TEXT_MAP = dict([(ord(char), None) for char in '[]{}'] + [(ord(char), ' ') for char in '|=*\\#'])
STOPWORDS = stopwords.words('english')

DATA_PATH = 'data'
ZIP_FILE_PATH = '%s/info_ret.zip' % DATA_PATH
XML_FILE_PATH = ZIP_FILE_PATH.replace('.zip', '.xml')
DATASET_URL = "http://cs.nyu.edu/courses/spring17/CSCI-GA.3033-006/assignment2_files/info_ret.zip"

def clean_text(text):
    # MD - This is unnecessary
    text = re.sub(r'\{\{.*?\}\}', '', text, flags=re.S)
    text = re.sub(r'<ref>.*?</ref>', '', text, flags=re.S)
    text = re.sub(r'\[\[File:.*?\|.*?\|.*?\|(.*?)\]\]', r'\1', text, flags=re.S)
    text = BeautifulSoup(text, 'lxml').get_text()
    text = text.translate(UGLY_TEXT_MAP)
    text = text.replace("'''", '"').replace("''", '"')
    text = text.strip()
    return text

def get_counter(title, text):
    chunk = ' '.join([title] * TITLE_BONUS) + ' ' + text
    term_list = [word.lower() for word in  nltk.word_tokenize(chunk) if word.lower() not in STOPWORDS]
    return Counter(term_list)

def build_index(filename):
    log.info("Building index from %s", filename)
    doc_list = minidom.parse(filename).getElementsByTagName('page')

    index_shards = [defaultdict(list) for _ in range(inventory.NUM_INDEX_SHARDS)]
    doc_shards = [defaultdict(dict) for _ in range(inventory.NUM_DOC_SHARDS)]
    df = defaultdict(int)
    for doc_id, page in enumerate(doc_list):
        page = doc_list[doc_id]
        title = page.getElementsByTagName('title')[0].childNodes[0].nodeValue
        if title.startswith('Category:'):
            continue
        text = clean_text(page.getElementsByTagName('text')[0].childNodes[0].nodeValue)
        tf = get_counter(title, text)

        doc_shards[doc_id % len(doc_shards)][doc_id] = (title, text)
        for term in tf.keys():
            index_shards[doc_id % len(index_shards)][term].append((doc_id, tf[term]))
            df[term] += 1

    log_idf = defaultdict(float)
    for term in df.keys():
        log_idf[term] = math.log(len(doc_list) / float(df[term]))

    pickle.dump(log_idf, open(inventory.DF_STORE, 'wb'))
    for ix, index_shard in enumerate(index_shards):
        pickle.dump(index_shard, open(inventory.POSTINGS_STORE % ix, 'wb'))
    for ix, doc_shard in enumerate(doc_shards):
        pickle.dump(doc_shard, open(inventory.DOCS_STORE % ix, 'wb'))

def main():
    if not os.path.exists(XML_FILE_PATH):
        log.info("Fetching %s", DATASET_URL)
        download_stream = urlopen(DATASET_URL)
        with open(ZIP_FILE_PATH, "wb") as zip_file:
            zip_file.write(download_stream.read())
        log.info("Extracting %s", ZIP_FILE_PATH)
        zipfile.ZipFile(ZIP_FILE_PATH).extractall(DATA_PATH)
    build_index(XML_FILE_PATH)

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.DEBUG)
    main()
