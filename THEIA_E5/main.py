from freqDB import *
import os
import sys
import json
import ast
from config import *
import time

train_kname = artifact_path + "day_8_9"
trainFilePath = artifact_path + "day_8_9.csv"

testFilePath = artifact_path + "day_15_15.csv"
kname = artifact_path + "day_15_15"

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

    print("testing and extracting kpaths")
    df_test, dfList_test, graphNames_test = readPandasFile(testFilePath, names=names, sep=',')
    freqDict = readFromFile(train_kname + '_freqList.data')
    setOfsets = readFromFile(train_kname + '_setOfsets.data')
    model = Word2Vec.load(train_kname + "_word2vec_model.model")
    path_vectors = np.load(train_kname + "_path_vectors.npy")
    max_freq = getMaxfreq(freqDict)
    graphs = seperate(df_test)
    final_paths = {}
    anomalous_path2score = {}
    k1 = 10
    k2 = 10
    graph = graphs[0]
    graphName = graph['graphId'].iloc[0]
    graph = toList(graph)
    avg_tokens = get_token_count(setOfsets)

    adjListForward, adjListBackward, anomalous_node = createAdjListCleanly(graph, setOfsets, freqDict, max_freq, id_map, model, path_vectors, avg_tokens)
    anomalous_node_name = set()
    for n in anomalous_node:
        anomalous_node_name.add(id_map['name_map'][n[0]])
    writeToFile(anomalous_node, kname + '_anomalous_nodes_1.data')

    adjForward = sortTime(adjListForward)
    forAdj, backAdj = makeAdjListDAGFaster(adjForward)
    for_ff = kname + '_forAdj.data'
    back_ff = kname + '_backAdj.data'
    writeToFile(forAdj, for_ff)
    writeToFile(backAdj, back_ff)

    anomalous_all_paths, anomalous_path2score, anomalous_all_paths2index, anomalous_all_paths2itime = findKAnomlousPaths(forAdj, backAdj, k1, k2, anomalous_node, graphName, id_map, anomalous_path2score)

    final_paths = findnetflownode(anomalous_all_paths, id_map, final_paths)
    key_nodes = find_key_nodes(final_paths)
    writeToFile(key_nodes, kname + '_key_nodes.data')
    writeToFile(final_paths, kname + '_anomalous_path.data')
    writeToFile(anomalous_path2score, kname + '_anomalous_path2score.data')
    writeToFile(anomalous_all_paths2index, kname + '_anomalous_path2index.data')
    writeToFile(anomalous_all_paths2itime, kname + '_anomalous_path2time.data')

main()
