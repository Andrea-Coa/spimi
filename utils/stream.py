import pickle
import re
import sys
import nltk
from nltk.stem.snowball import SnowballStemmer
# nltk.download('punkt')

# DEBUG

stemmer = SnowballStemmer('spanish')
# LIMIT = 400 # bytes

class Stream:

    def preprocess(self, contents, stoplist, lang = "english"):
        words = []
        contents = contents.lower()
        contents = re.sub(r'[^a-zA-Z0-9_À-ÿ]', ' ', contents)
        words = nltk.word_tokenize(contents, language=lang)
        words = [word for word in words if word not in stoplist]    
        words = [stemmer.stem(word) for word in words]
        return words

    def __init__(self, doclist, stoplist, limit):
        self.list_count = 0      # cantidad de listas que hay
        self.list_idx = 0        # índice de lista actual de terms
        self.current_list = []   # lista actual de terms
        self.current_idx = 0    # índice actual la lista de terms
        self.file = None
        self.empty = False


        idoc = -1   # documento procesando actualmente
        stems = []  # stems del documento
        istem = 0   # stem actual
        chunk = []  # lista de pares a escribir en archivo
        not_all_documents_have_been_processed = True
        file = open("stream.pkl", "wb")

        while not_all_documents_have_been_processed:
            # si ya completamos todos los stems de un archivo
            if istem >= len(stems):
                if idoc < len(doclist) and idoc >= 0:
                    print("Document " + doclist[idoc] + " has been preprocessed!")
                idoc += 1

                # y si ya no hay más documentos por procesar
                if idoc >= len(doclist):
                    if len(chunk) > 0:
                        pickle.dump(chunk, file)
                        self.list_count += 1
                        chunk = []
                        stems = []
                    not_all_documents_have_been_processed = False

                # aún quedan más documentos!
                else:
                    with open(doclist[idoc]) as f:
                        contents = f.read()
                    stems = self.preprocess(contents, stoplist)

                # stems = [] # reiniciamos 
                istem = 0
            
            # escribir los stems
            while istem < len(stems):
                if sys.getsizeof(chunk) >= limit:
                    pickle.dump(chunk, file)
                    self.list_count += 1
                    chunk = []
                chunk.append((stems[istem], doclist[idoc]))
                istem += 1
        
        file.close()
        self.file = open("stream.pkl", "rb")
        
        if self.current_idx < self.list_count:
            self.current_list = pickle.load(self.file)

    def next(self):
        # si ya agotamos la lista de pares actual
        if self.current_idx >= len(self.current_list):

            self.list_idx += 1 # llevar la cuenta de cuántos chunks hemos leído
            
            # y si ya agotamos todas las listas
            if self.list_idx >= self.list_count:
                self.file.close()
                self.empty = True
                return (-1, -1)
            
            # cargar el siguiente chunk de pares
            self.current_list = pickle.load(self.file)
            self.current_idx = 0 # ahora estamos en la posición 0 de la nueva lista

        # si ya alcanzamos el último elemento, entonces queda vacío
        # if self.current_idx == len(self.current_list) -1:
        #     self.empty = True
        res = self.current_list[self.current_idx]
        self.current_idx += 1
        return res

    def end(self):
        self.file.close()
    
    def open_file(self):
        self.file = open("stream.pkl", "rb")

    # def __del__(self):
        # self.file.close()