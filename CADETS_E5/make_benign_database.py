from freqDB import *
import os
import sys
import json
import ast
from config import *

train_kname = artifact_path + "day_8_11"
trainFilePath = artifact_path + "day_8_11.csv"

id_map_path = artifact_path + database + '_map' + '.json'


def read(path):
    data = json.load(open(path, 'r'))
    return data
def writeToFile(data, filename):
    with open(filename, 'w') as file:
        file.write(str(data))

def readFromFile(filename):
    with open(filename, 'r') as file:
        data_str = file.read()
        data = ast.literal_eval(data_str)
        return data

def main(names=("sourceId", "sourceType", "destinationId", "destinationType", "syscal", "retTime","graphId", "freq", "spid")):
    id_map = read(id_map_path)
    df_train, dfList_train, graphNames_train = readPandasFile(trainFilePath, names=names, sep=',')
    freqDict, setOfsets, nodes_sum = createFreqDict(dfList_train, graphNames_train, id_map, 6)
    writeToFile(freqDict, train_kname + '_freqList.data')
    writeToFile(setOfsets, train_kname + '_setOfsets.data')
    model, paths_vectors = find_common_string(setOfsets, nodes_sum)
    print("writing freq to file")
    model.save(train_kname + "_word2vec_model.model")
    np.save(train_kname + "_path_vectors.npy", paths_vectors)


main()
