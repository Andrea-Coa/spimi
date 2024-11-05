from stream import Stream
import importlib
import os
import sys
import pickle

MAX_DICT_SIZE = 1000
directory = "D:/Documents-D/Testing/spimi_files_to_merge"

# SPIMI-INVERT:
# -------------
# Crea un nuevo archivo y un nuevo hash table, o diccionario. Mientras
# el diccionario tenga espacio (y el stream tenga elementos), agrega los 
# pares que provee el stream.

def spimi_invert(directory : str, ss : Stream, n : int):
    term_dict = {}
    term_dict_size = sys.getsizeof(term_dict)
    while term_dict_size < MAX_DICT_SIZE:
        if ss.empty:
            # print("stream got empty!")
            break
        token = ss.next()
        if token[0] == -1:
            break
        # print("token: ", token)
        term = str(token[0])
        docID = token[1]

        if term not in term_dict:
            term_dict[term] = []
            term_dict[term].append((docID, 1))
        else:
            postings_list = term_dict[term]
            found = False
            # add to postings list
            for i in range(len(postings_list)):
                if postings_list[i][0] == docID:
                    count = postings_list[i][1]
                    postings_list[i] = (docID, count + 1)
                    found = True
                if found:
                    break
            if not found:
                postings_list.append((docID, 1))
        term_dict_size = sys.getsizeof(term_dict)
    # sort
    term_dict = dict(sorted(term_dict.items()))
    filename = directory + "f" + str(n) + ".pkl"
    with open(filename, "wb") as file:
        pickle.dump(term_dict, file)
    return filename
    

if __name__ == "__main__":

    # load stoplist
    with open("stoplist.txt", encoding="latin1") as file:
        stoplist = [line.rstrip().lower() for line in file]

    # create stream of tokens (term-docID)
    files = all_files_and_dirs = os.listdir("minitexts/")
    files = ["minitexts/" + file for file in files]
    ss = Stream(files, stoplist)

    # SPIMIndexConstruction!
    files_to_merge = []
    n = 0
    while not ss.empty:
        fn = spimi_invert(ss, n)
        files_to_merge.append(fn)
        n+=1

    print(files_to_merge)
