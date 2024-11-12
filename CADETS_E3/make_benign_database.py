from freqDB import *
import os
import sys
import json
import ast
from config import *

train_kname = artifact_path + "day_2_4"
trainFilePath = artifact_path + "day_2_4.csv"

id_map_path = artifact_path + "tc_e3_cadet_dataset_db_map.json"


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
    print("reading datasets")
    df_train, dfList_train, graphNames_train = readPandasFile(trainFilePath, names=names, sep=',')
    print("generating freq db")
    freqDict, setOfsets, nodes_sum = createFreqDict(dfList_train, graphNames_train, id_map, 6)
    print(len(setOfsets))
    writeToFile(freqDict, train_kname + '_freqList.data')   # 这里面记录的是每个src和dst交互的次数
    writeToFile(setOfsets, train_kname + '_setOfsets.data')  # 记录所有实体的msg
    model, paths_vectors = find_common_string(setOfsets, nodes_sum)
    model.save(train_kname + "_word2vec_model.model")
    np.save(train_kname + "_path_vectors.npy", paths_vectors)


main()