import numpy as np
import pandas as pd
import math
import networkx as nx
import re
from gensim.models import Word2Vec
from sklearn.cluster import KMeans
from tqdm import tqdm
from collections import deque

def createFreqDict(parsedList, listOfGraphs, id_map, graphIndex=6):
    setOfsets = [{}, {}]
    freqDict = {}
    for row in parsedList:
        setPerTime(row, setOfsets, id_map)
        src, dest, count = id_map['name_map'][row[0]], id_map['name_map'][row[2]], row[7]
        if type(src) != str and np.isnan(src):
            src = 'None'
        if type(dest) != str and np.isnan(dest):
            dest = 'None'
        srcRel = (src, row[4])
        if srcRel not in freqDict:
            freqDict[srcRel] = {}
            freqDict[srcRel]['total'] = 0
        if dest not in freqDict[srcRel]:
            freqDict[srcRel][dest] = 0
        freqDict[srcRel][dest] += count
        freqDict[srcRel]['total'] += count

    return freqDict, setOfsets

def setPerTime(row, setOfsets, id_map):

    src, dest = id_map['name_map'][row[0]], id_map['name_map'][row[2]]
    if type(src) != str and np.isnan(src):
        src = 'None'
    if type(dest) != str and np.isnan(dest):
        dest = 'None'
    if src not in setOfsets[0]:
        setOfsets[0][src] = 0
    else:
        setOfsets[0][src] += 1
    if dest not in setOfsets[1]:
        setOfsets[1][dest] = 0
    else:
        setOfsets[1][dest] += 1

def seperate(df):
    gb = df.groupby('graphId')
    graphs = [gb.get_group(x) for x in gb.groups]
    return graphs


def readPandasFile(parsedFile, names = ("sourceId", "sourceType", "destinationId", "destinationType", "syscal", "retTime","graphId", "freq", "spid"), sep=','):
    parsedDf = pd.read_csv(parsedFile, names=list(names), sep=sep)
    parsedDf['sourceId'] = parsedDf['sourceId'].astype(str)
    parsedDf['destinationId'] = parsedDf['destinationId'].astype(str)
    parsedDf['syscal'] = parsedDf['syscal'].str.strip('" \n\t')
    parsedDf['sourceType'] = parsedDf['sourceType'].str.strip('" \n\t')
    parsedDf['destinationType'] = parsedDf['destinationType'].str.strip('" \n\t')
    parsedList = parsedDf.values.tolist()
    print(parsedDf.graphId.unique())
    uniqueGraphNames = sorted(list(parsedDf.graphId.unique()))
    return parsedDf, parsedList, uniqueGraphNames

def getMaxfreq(freqDict):
    max_freq = 0
    for n, p in freqDict.items():
        if p['total'] > max_freq:
            max_freq = p['total']

    return max_freq

def getInScore(src, setOfsets):
    total_sum = sum(setOfsets[0].values())
    if src in setOfsets[0]:
        count = setOfsets[0][src]
    else:
        count = 0

    return count / total_sum
def getOutScore(dest, setOfsets):
    total_sum = sum(setOfsets[1].values())
    if dest in setOfsets[1]:
        count = setOfsets[1][dest]
    else:
        count = 0
    return count / total_sum

def getFreqScore (src, dest, syscal, freqDict, max_freq):
    srcRel = (src, syscal)
    if srcRel not in freqDict:
        return 1 / max_freq
    if dest not in freqDict[srcRel]:
        return 1 / max_freq
    return freqDict[srcRel][dest] / freqDict[srcRel]['total']

def toList(df):
    return df.values.tolist()

def get_path_vector(path, model):
    words = path.split('/')
    word_vectors = [model.wv[word] for word in words if word in model.wv]
    path_vector = np.mean(word_vectors, axis=0)
    return path_vector

def get_target_node(parsedList, target_node_name, id_map):
    node_count = {}

    for row in parsedList:
        src = id_map['name_map'][row[0]]
        dest = id_map['name_map'][row[2]]

        if src not in node_count:
            node_count[src] = 0
        else:
            node_count[src] += 1

        if dest not in node_count:
            node_count[dest] = 0
        else:
            node_count[dest] += 1

    sum_count = 0
    for k, c in node_count.items():
        sum_count += c

    avg_count = sum_count // len(node_count)
    for k, c in node_count.items():
        if c > avg_count:
            target_node_name.add(k)

    return target_node_name

def sortTime(adjDict):
    for key in adjDict:
        adjDict[key] = sorted(adjDict[key])
    return adjDict

def extract_ip_address(text):
    pattern = r'(?<=->)((25[0-5]|2[0-4]\d|1?\d{1,2})\.){3}(25[0-5]|2[0-4]\d|1?\d{1,2})'
    match = re.search(pattern, text)
    if match:
        return match.group(0)
    return None

def calculateScore(src, dest, syscal, setOfsets, freqDict, max_freq, id_map, target_node_name, target_node):
    src = list(src)
    dest = list(dest)
    retVal = None
    src_id = id_map['name_map'][src[0]]
    dest_id = id_map['name_map'][dest[0]]

    if type(src[0]) != str and np.isnan(src[0]):
        retVal = math.log2(0.5)*-1
    if type(dest[0]) != str and np.isnan(dest[0]):
        retVal = math.log2(0.5)*-1
    if retVal is None:
        inScore = getInScore(src_id, setOfsets)
        outScore = getOutScore(dest_id, setOfsets)
        freqScore = getFreqScore(src_id, dest_id, syscal, freqDict, max_freq)

        if src_id in target_node_name and len(src_id.split('/')) != 1 and src not in target_node:
            target_node.append(src)

        if dest_id in target_node_name and len(dest_id.split('/')) != 1 and dest not in target_node:
            target_node.append(dest)

        if outScore == 0:
            outScore = 1/sum(setOfsets[1].values())

        if inScore == 0:
            inScore = 1/sum(setOfsets[0].values())

        retVal = math.log2(inScore * freqScore * outScore) * -1

    return retVal * -1


def addToAdjList(s, d, edge, adjListForward, adjListBackward):

    adjListForward.setdefault(s, [])
    adjListForward[s].append((edge[0], d, edge[1], edge[2]))

    adjListBackward.setdefault(d, [])
    adjListBackward[d].append((edge[0], s, edge[1], edge[2]))


def createAdjListCleanly(parsedList, setOfsets, freqDict, max_freq, id_map, target_node_name):
    adjListForward = {}
    adjListBackward = {}
    target_node = []
    for i in tqdm(range(len(parsedList))):
        row = parsedList[i]

        src, dest = (row[0], row[1], 0), (row[2], row[3], 0)
        if row[1] != 'subject':
            dest = (row[2], row[6], row[3], 0)
        elif row[3] != 'subject':
            src = (row[0], row[6], row[1], 0)
        else:
            src = (row[0], row[6], row[1], 0)
            dest = (row[2], row[6], row[3], 0)

        score = calculateScore((row[0], row[1]), (row[2], row[3]), row[4], setOfsets, freqDict, max_freq, id_map, target_node_name, target_node)
        addToAdjList(src, dest, (row[5], row[4], score), adjListForward, adjListBackward)

    return adjListForward, adjListBackward, target_node



def makeAdjListDAGFaster(adjListForward):
    forwardEdges = []
    dagForAdj = {}
    dagDestAdj = {}
    for src in adjListForward:
        for edge in adjListForward[src]:
            forwardEdges.append((edge[0], src, edge[1], edge[2], edge[3]))
    forwardEdges = sorted(forwardEdges)
    for edge in forwardEdges:
        src = edge[1]
        dest = edge[2]
        edgeAttributes = (edge[0], edge[3], edge[4])

        if src not in dagForAdj:
            dagForAdj[src] = []
        dagForAdj[src].append((dest, edgeAttributes))

        if dest not in dagDestAdj:
            dagDestAdj[dest] = []
        dagDestAdj[dest].append((src, edgeAttributes))

    return dagForAdj, dagDestAdj


def backward_find_k1(adj, node, depth, paths_count, nebor_count):

    queue = deque([(node, 0, 0, [node], [0], [])])
    visited = set([(node, 0, 0)])
    paths = []
    scores = []
    times = []
    events = []

    while queue:
        if len(paths) > paths_count:
            break
        current_node, current_time, current_score, path, time, event = queue.popleft()
        neighbors = adj.get(current_node, [])
        neighbors = sorted(neighbors, key=lambda x: x[1][-1], reverse=True)

        if not neighbors:
            scores.append(current_score)
            paths.append(path)
            times.append(time)
            events.append(event)
            continue
        count = 0        
        for neighbor, attrs in neighbors:
            next_time, event_type, score = attrs
            state = (neighbor, next_time, event_type)

            if state not in visited and count <= nebor_count:
                if current_time == 0 or next_time <= current_time:
                    visited.add(state)
                    new_path = path + [neighbor]
                    new_time = time + [next_time]
                    new_event = event + [event_type]
                    new_score = current_score + score
                    count += 1

                    if len(new_path) - 1 == depth:
                        if new_path not in paths:
                            scores.append(new_score)
                            paths.append(new_path)
                            times.append(new_time)
                            events.append(new_event)
                    else:
                        queue.append((neighbor, next_time, new_score, new_path, new_time, new_event))

    return paths, scores, times, events


def forword_find_k1(adj, node, depth, paths_count, nebor_count):

    queue = deque([(node, 0, 0, [node], [0], [])])
    visited = set([(node, 0, 0)])
    paths = []
    scores = []
    times = []
    events = []

    while queue:
        if len(paths) > paths_count:
            break
        current_node, current_time, current_score, path, time, event = queue.popleft()
        neighbors = adj.get(current_node, [])
        neighbors = sorted(neighbors, key=lambda x: x[1][-1], reverse=True)

        if not neighbors:
            scores.append(current_score)
            paths.append(path)
            times.append(time)
            events.append(event)
            continue
        count = 0    
        for neighbor, attrs in neighbors:
            next_time, event_type, score = attrs
            state = (neighbor, next_time, event_type)

            if state not in visited and count <= nebor_count:
                if current_time == 0 or next_time >= current_time:
                    visited.add(state)
                    new_path = path + [neighbor]
                    new_time = time + [next_time]
                    new_event = event + [event_type]
                    new_score = current_score + score
                    count += 1

                    if len(new_path) - 1 == depth:
                        if new_path not in paths:
                            scores.append(new_score)
                            paths.append(new_path)
                            times.append(new_time)
                            events.append(new_event)
                    else:
                        queue.append((neighbor, next_time, new_score, new_path, new_time, new_event))

    return paths, scores, times, events

def findKAnomlousPaths(forAdj, backAdj, k1, k2, anomalous_node, graphName, id_map):

    k3 = 3
    anomalous_all_paths = {}
    for i in tqdm(range(0, len(anomalous_node))):
        a_n = anomalous_node[i]
        a_n_anomalous_paths = {}
        a_n_anomalous_edges = {}
        a_n_anomalous_score = {}

        if a_n[1] == 'subject':
            src = (a_n[0], graphName, a_n[1], 0)
        else:
            src = (a_n[0], a_n[1], 0)

        if src in forAdj:
            forward_paths, forward_scores, forward_times, forward_events = forword_find_k1(forAdj, src, k1, k2, k3)
        else:
            forward_paths, forward_scores, forward_times, forward_events = [], [], [], []
        if src in backAdj:
            backward_paths, backward_scores, backward_times, backward_events = backward_find_k1(backAdj, src, k1, k2, k3)
        else:
            backward_paths, backward_scores, backward_times, backward_events = [], [], [], []

        if len(forward_paths) != 0 and len(backward_paths) == 0:
            for i in range(len(forward_scores)):
                a_n_anomalous_score[i] = forward_scores[i]
                a_n_anomalous_paths[i] = forward_paths[i]
                a_n_anomalous_edges[i] = forward_events[i]


        elif len(forward_paths) == 0 and len(backward_paths) != 0:
            for i in range(len(backward_scores)):
                a_n_anomalous_score[i] = backward_scores[i]
                a_n_anomalous_paths[i] = backward_paths[i][::-1]
                a_n_anomalous_edges[i] = backward_events[i][::-1]

        else:
            for_start_time = [t[1] for t in forward_times]
            back_start_time = [t[1] for t in backward_times]

            for i in range(len(back_start_time)):
                for j in range(len(for_start_time)):
                    if back_start_time[i] < for_start_time[j]:
                        a_n_anomalous_score[i] = forward_scores[j] + backward_scores[i]
                        a_n_anomalous_paths[i] = backward_paths[i][::-1] + forward_paths[j][1:]
                        a_n_anomalous_edges[i] = backward_events[i][::-1] + forward_events[j]

        sort_scores = dict(sorted(a_n_anomalous_score.items(), reverse=True, key=lambda item: item[1])).keys()
        sort_scores = list(sort_scores)

        src_id = id_map['name_map'][a_n[0]]
        anomalous_all_paths[(a_n[0], src_id)] = []

        for s in sort_scores:
            path = []
            count = 0
            for j in range(len(a_n_anomalous_paths[s])):
                dst_id = id_map['name_map'][a_n_anomalous_paths[s][j][0]]
                path.append(dst_id)
                if count != len(a_n_anomalous_paths[s]) - 1:
                    path.append(a_n_anomalous_edges[s][j])
                count += 1
            if path not in anomalous_all_paths[(a_n[0], src_id)]:
                anomalous_all_paths[(a_n[0], src_id)].append(path)

    return anomalous_all_paths
