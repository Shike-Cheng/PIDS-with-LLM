import csv
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import numpy as np
from config import *
import ast

def readFromFile(filename):
    with open(filename, 'r') as file:
        data_str = file.read()
        data = ast.literal_eval(data_str)
        return data

def data2csv(data_paths):
    new_data = []
    sum_paths = []

    for k, paths in data_paths.items():
        for p in paths:
            if p not in sum_paths:
                sum_paths.append(p)
    
    for p in sum_paths:
        new_data.append([str(p), "benign"])
    
    with open(artifact_path + "day_2_4_path.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in new_data:
            writer.writerow(row)

if __name__=='__main__':
    data_paths = readFromFile(artifact_path + "day_2_4_path.data")
    data2csv(data_paths)
    sentences = []
    labels = []
    mal_nodes = []
    with open(artifact_path + "day_2_4_path.csv", newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            s = ""
            count = 0
            for n in eval(row[0]):
                if count == len(eval(row[0])) - 1:
                    s += n
                else:
                    s += n + ' '
                count += 1
            sentences.append(s)
            labels.append(row[1])
            if len(row) == 3:
                mal_nodes.append(eval(row[2]))
            else:
                mal_nodes.append([])
    
    
    tagged_data = [TaggedDocument(words=sentence.split(), tags=[str(i)]) for i, sentence in enumerate(sentences)]
    
    model = Doc2Vec(vector_size=50, alpha=0.025, min_count=1, dm=1, epochs=100)
    model.build_vocab(tagged_data)
    model.train(tagged_data, total_examples=model.corpus_count, epochs=model.epochs)
    
    
    model.save(artifact_path + "day_2_4_path.model")
    model = Doc2Vec.load(artifact_path + "day_2_4_path.model")
    
    def get_sentence_embedding(sentence):
        return model.infer_vector(sentence.split())
    
    
    embeddings = [get_sentence_embedding(sentence) for sentence in sentences]
    np.save(artifact_path + 'day_2_4_path.npy', np.array(embeddings))


