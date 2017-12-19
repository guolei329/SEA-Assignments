from gensim.models import word2vec
import itertools

def init_model(input_files):
    sentences = itertools.chain.from_iterable([open(f) for f in input_files])
    model = word2vec.Word2Vec([_.split() for _ in sentences], size=48, iter=0, negative=20)
    return model

def compute_gradient(model, input_file):
    syn0, syn1 = model.wv.syn0.copy(), model.syn1neg.copy()
    with open(input_file) as f:
        model.train(word2vec.LineSentence(f), total_examples=model.corpus_count//3, epochs=3)
    return {'syn0': model.wv.syn0 - syn0, 'syn1': model.syn1neg - syn1}

def update_model(model, gradient):
    model.wv.syn0 += gradient['syn0']
    model.syn1neg += gradient['syn1']
