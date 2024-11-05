import pickle
from math import log2
import re
import nltk
import numpy as np
import heapq
from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer('english')



class QueryRunner:
    def __init__(self, index_path, doclist, idfs, norms, stoplist):
        self.index_path = index_path
        self.doclist = doclist
        self.idfs = idfs
        self.index = {}
        self.norms = norms
        self.stoplist = stoplist
        with open(index_path, "rb") as f:
            self.index = pickle.load(f)

    def preprocess(self, contents, stoplist, lang = "english"):
        words = []
        contents = contents.lower()
        contents = re.sub(r'[^a-zA-Z0-9_À-ÿ]', ' ', contents)
        words = nltk.word_tokenize(contents, language=lang)
        words = [word for word in words if word not in stoplist]    
        words = [stemmer.stem(word) for word in words]
        return words
    
    def cosine_similarity(self, q, p, docid):
        return np.sum(q * p) / (np.linalg.norm(q) * self.norms[docid])

    def make_query(self, query : str, k = 10):
        stems = self.preprocess(query, self.stoplist)
        # tfs
        stems_unique = {}
        for stem in stems:
            if stem in self.idfs:
                if stem not in stems_unique:
                    stems_unique[stem] = 1
                else:
                    stems_unique[stem] += 1
        # tf-idfs
        for stem, tf in stems_unique.items():
            idf = self.idfs[stem]
            tf = log2(1 + tf)
            stems_unique[stem] = tf * idf

        # query vector
        qvec = list(stems_unique.values())
        vals = list(stems_unique.keys())
        idx = {stems[i]:i for i in range(len(vals))}
        docmap = {self.doclist[i]:i for i in range(len(self.doclist))}
        del stems_unique

        # construir vectores para cada documento
        doc_vectors = np.zeros((len(self.doclist), len(qvec)))
        for i in range(len(vals)):
            postings_list = self.index[vals[i]]
            for docname, tfidf in postings_list:
                idoc = docmap[docname]
                doc_vectors[idoc][i] = tfidf

        # calcular similitud de coseno
        knn = []
        for i in range(len(doc_vectors)):
            docid = self.doclist[i]
            d = self.cosine_similarity(qvec, doc_vectors[i], docid)
            if len(knn) < k:
                heapq.heappush(knn, (d, docid))
            else:
                if d > knn[0][0]:
                    heapq.heappop(knn)
                    heapq.heappush(knn, (d, docid))


        knn = sorted(knn, key=lambda x: x[0], reverse=True)        
        for i in range(len(knn)):
            knn[i] = (knn[i][1], knn[i][0])
        return knn
