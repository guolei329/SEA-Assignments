import nltk
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from collections import Counter

TITLE_BONUS = 50

REMOVE_PUNCT_MAP = dict((ord(char), None) for char in "=[]\\")
UGLY_TEXT_MAP = dict((ord(char), ord(" ")) for char in "|=[]{}*\\#")
STOP = stopwords.words('english')

def clean_text(text):
    text = BeautifulSoup(text, 'lxml').get_text()
    text = text.translate(UGLY_TEXT_MAP)
    text = text.replace("'''", '"')
    return text

def get_counter(title, text):
    chunk = " ".join([title] * TITLE_BONUS) + " " + text
    no_punct_chunk = chunk.translate(REMOVE_PUNCT_MAP)
    term_list = [word.lower() for word in nltk.word_tokenize(no_punct_chunk) if word.lower() not in STOP]
    return Counter(term_list)

def doc_iterator(stdin):
    for line in stdin:
        doc_id, title, text = line.strip().split("\t", 2)
        if title.startswith("Category:"):
            continue
        text = clean_text(text)
        tf = get_counter(title, text)
        yield int(doc_id), title, text, tf
