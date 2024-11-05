import pickle
import sys
import os

# CLASS READBUFFER
# ------------
# Read buffer para facilitar el merge sort

# CLASS WRITEBUFFER:
# ------------------
# Write buffer para escribir resultados del merge sort

class ReadBuffer:
    # files: lista de archivos que se leerán en esta "ronda"
    # ifile: índice del archivo cuyo contenido tenemos en memoria principal actualmente
    # invidx: índice invertido (lista de tuplas), contenio del archivo files[ifile]
    # i: indica posición actual en el índice invertido
    # over: booleano que indica si ya se terminó de leer todas las postings_lists
    # word: palabra actual
    # idoc: índice del documento actual del postings_list de la palabra actual
    # docID: docID del documento actual del postings_list de la palabra actual

    def __init__(self, directory, start, end, n ): # n es cantidad total de files que existen
        self.files = []
        # print(start, end, n)
        for i in range(start, end):
            if i >= n:
                break
            self.files.append(directory + "f" + str(i) + ".pkl")
        
        self.ifile = 0
        if self.ifile < len(self.files):
            with open(self.files[self.ifile], "rb") as f:
                dict = pickle.load(f)
            os.remove(self.files[self.ifile])
            print("read file ", self.files[self.ifile])
            self.invidx = list(dict.items())
            self.over = False
            self.word = self.invidx[0][0]
            self.idoc = 0
            self.docID = self.invidx[0][1][self.idoc][0]
            self.count = self.invidx[0][1][self.idoc][1]
        else:
            self.invidx = []
            self.over = True
        self.i = 0

    # ver el contenido actual sin quitarlo
    def peep(self):
        if self.over:
            return (-1, -1)
        return self.word, (self.docID, self.count)
    
    # extraer el contenido actual
    def get_next_pair(self):
        if self.over:
            return (-1, (-1, -1))
        i = self.i
        word = self.word
        docId = self.docID
        count = self.count
        postings_list = self.invidx[i][1]
        self.idoc += 1
        
        # si estamos al final de la postings_list
        if self.idoc >= len(postings_list) : 

            # si estamos al final del índice invertido: leemos el siguiente índice invertido
            if i >= len(self.invidx) - 1:

                # pero en caso no hay un siguiente: ya se acabaron los archivos
                self.ifile += 1
                if self.ifile == len(self.files): 
                    self.over = True
                else:
                    # si aún no se acaban los archivos, leemos el siguiente y reiniciamos variables
                    with open(self.files[self.ifile], "rb") as f:
                        dict = pickle.load(f)
                    os.remove(self.files[self.ifile])
                    print("read file ", self.files[self.ifile])
                    self.i = 0
                    self.invidx = list(dict.items())
                    if not dict:
                        self.over = True
                    else:
                        self.word = self.invidx[0][0]
                        self.idoc = 0
                        self.docID = self.invidx[0][1][self.idoc][0]
                        self.count = self.invidx[0][1][self.idoc][1]

            # si aún no llegamos al final del índice invertido actual
            else:
                self.i += 1
                self.idoc = 0
                self.word = self.invidx[self.i][0]
                self.docID = self.invidx[self.i][1][self.idoc][0]
                self.count = self.invidx[self.i][1][self.idoc][1]

        # si no estamos al final de postings_list
        else: 
            # self.idoc += 1
            # self.word no cambia
            # print(self.invidx[0][1])
            self.docID = self.invidx[self.i][1][self.idoc][0]
            self.count = self.invidx[self.i][1][self.idoc][1]
        
        return word, (docId, count)


class WriteBuffer:
    def __init__(self, directory, start, end, n, max_dict_size):
        self.max_dict_size = max_dict_size
        self.contents = {}
        self.files = [ directory + "f" + str(i) + ".pkl" for i in range(start, end) if i < n]
        self.ifile = 0
        print("WRITE BUFFER FILES: ", self.files)

    def write_to_file(self):
        if self.ifile >= len(self.files) or not self.contents:
            return 0
        with open(self.files[self.ifile], "wb") as file:
            pickle.dump(self.contents, file)
        self.contents = {}
        print("--> wrote ", self.files[self.ifile])
        self.ifile += 1
        return 1
    
    # garantizar que se actualicen todos los archivos...
    def end_round(self):
        self.write_to_file()
        self.contents = {}
        while self.ifile < len(self.files):
            with open(self.files[self.ifile], "wb") as file:
                pickle.dump(self.contents, file)
            print("--> wrote ", self.files[self.ifile])
            self.ifile += 1

    def insert(self, word : str, elem):
        if self.ifile >= len(self.files):
            return 0
        if word not in self.contents:
            self.contents[word] = [elem]
        else:
            postings_list = self.contents[word]
            found = False
            for i in range(len(postings_list)):
                docname = elem[0]
                count = elem[1]
                if postings_list[i][0] == docname:
                    postings_list[i] = (docname, postings_list[i][1] + count)
                    found = True
            if not found:
                postings_list.append(elem)
        if sys.getsizeof(self.contents) > self.max_dict_size:
            return self.write_to_file()
        return 1