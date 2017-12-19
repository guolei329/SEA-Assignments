import os, json
os.environ['MKL_NUM_THREADS'] = '1' # See https://github.com/joblib/joblib/issues/138
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_DYNAMIC'] = 'FALSE'
from tornado import web
from collections import defaultdict
import numpy as np
from sklearn.metrics.pairwise import linear_kernel

class Index(web.RequestHandler):
    def initialize(self, data):
        self._posting_lists, self._log_idf = data

    def head(self):
        self.finish()

    def get(self):
        query = self.get_argument('q', '')
        query_terms = query.lower().split()
        query_vector = np.array([[self._log_idf[term] for term in query_terms]])
        doc_vector_dict = defaultdict(lambda: np.array([0.0] * len(query_terms)))
        for i, term in enumerate(query_terms):
            for doc_id, tf in self._posting_lists[term]:
                doc_vector_dict[doc_id][i] = tf * self._log_idf[term]
        doc_ids = list(doc_vector_dict.keys())
        if len(doc_ids) == 0:
            self.finish(json.dumps({'postings': []}))
            return
        doc_matrix = np.zeros((len(doc_vector_dict), len(query_terms)))
        for doc_ix, doc_id in enumerate(doc_ids):
            doc_matrix[doc_ix][:] = doc_vector_dict[doc_id][:]
        sims = linear_kernel(query_vector, doc_matrix).flatten()
        best_doc_ixes = sims.argsort()[::-1]
        best_doc_sims = sims[best_doc_ixes]
        best_doc_ids = [doc_ids[doc_ix] for doc_ix in best_doc_ixes]

        postings = list(zip(best_doc_ids, best_doc_sims))
        self.finish(json.dumps({'postings': postings}))

