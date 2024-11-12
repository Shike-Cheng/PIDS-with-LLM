import csv
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import numpy as np
from config import *

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


