from select_path_util import *
import os
import sys
import json
import ast
from config import *

train_kname = artifact_path + "day_10_10"
trainFilePath = artifact_path + "day_10_10.csv"

testFilePath = artifact_path + "day_3_6.csv"
kname = artifact_path + "day_3_5"
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
    df_test, dfList_test, graphNames_test = readPandasFile(testFilePath, names=names, sep=',')
    freqDict, setOfsets = createFreqDict(dfList_test, graphNames_test, id_map, 6)
    writeToFile(freqDict, kname + '_freqList.data')
    writeToFile(setOfsets, kname + '_setOfsets.data')

    all_paths = {}
    key_node_string = readFromFile(train_kname + '_key_nodes.data')
    graphs = seperate(df_test)
    target_node_name = key_node_string
    count = 1
    k1 = 10
    k2 = 10
    for graph in graphs:
        count += 1
        graphName = graph['graphId'].iloc[0]
        graph = toList(graph)
        max_freq = getMaxfreq(freqDict)
        adjListForward, adjListBackward, target_node = createAdjListCleanly(graph, setOfsets, freqDict, max_freq, id_map, target_node_name)
        node_ff = kname + '_target_nodes.data'
        writeToFile(target_node, node_ff)
        adjForward = sortTime(adjListForward)
        forAdj, backAdj = makeAdjListDAGFaster(adjForward)
        for_ff = kname + '_forAdj.data'
        back_ff = kname + '_backAdj.data'
        writeToFile(forAdj, for_ff)
        writeToFile(backAdj, back_ff)

        all_paths = findKAnomlousPaths(forAdj, backAdj, k1, k2, target_node, graphName, id_map)
    f_name = kname + '_path.data'
    writeToFile(all_paths, f_name)

main()
