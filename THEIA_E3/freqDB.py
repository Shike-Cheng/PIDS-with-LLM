import numpy as np
import pandas as pd
import math
import networkx as nx
import re
from gensim.models import Word2Vec
from sklearn.cluster import KMeans
from tqdm import tqdm
from collections import deque
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from gensim.models import Word2Vec
from scipy.cluster.hierarchy import linkage, fcluster
from sklearn.preprocessing import normalize
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import random
from joblib import Parallel, delayed
from sklearn.metrics.pairwise import cosine_similarity
import time

import warnings

warnings.filterwarnings("ignore")



def get_path_vector(path, model):
    words = path.split('/')
    word_vectors = [model.wv[word] for word in words if word in model.wv]

    if not word_vectors:
        return np.zeros(model.vector_size)

    path_vector = np.mean(word_vectors, axis=0)
    return path_vector


def find_common_string(setOfsets, paths):
    sum_token = 0
    sum_sets = {}
    for p in paths:
        w = p.split('/')
        sum_token += len(w) - 1
        if p in setOfsets[0] and p in setOfsets[1]:
            sum_sets[p] = setOfsets[0][p] + setOfsets[1][p]
        elif p in setOfsets[0] and p not in setOfsets[1]:
            sum_sets[p] = setOfsets[0][p]
        else:
            sum_sets[p] = setOfsets[1][p]

    avg_tokens = sum_token // len(paths)
    sum_count = sum(sum_sets.values())
    avg_count = sum_count // len(paths)
    cluster_path = []
    for p in paths:
        w = p.split('/')
        if len(w) - 1 > avg_tokens or sum_sets[p] > avg_count:
            cluster_path.append(p)

    print(len(cluster_path))
    tokenized_paths = [path.split('/') for path in cluster_path]
    model = Word2Vec(sentences=tokenized_paths, vector_size=100, window=5, min_count=1, workers=4)
    path_vectors = np.array([get_path_vector(path, model) for path in cluster_path])
    path_vectors_normalized = normalize(path_vectors, norm='l2')

    return model, path_vectors_normalized


def createFreqDict(parsedList, listOfGraphs, id_map, graphIndex=6):
    setOfsets = [{}, {}]
    freqDict = {}
    nodes_set = set()
    for row in parsedList:
        setPerTime(row, setOfsets, id_map)
        src, dest, count = id_map['name_map'][row[0]], id_map['name_map'][row[2]], row[7]
        if id_map['type_map'][row[0]] != 'netflow':
            nodes_set.add(src)
        if id_map['type_map'][row[2]] != 'netflow':
            nodes_set.add(dest)
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

    return freqDict, setOfsets, nodes_set


def setPerTime(row, setOfsets, id_map):
    src, dest = id_map['name_map'][row[0]], id_map['name_map'][row[2]]

    if type(src) != str and np.isnan(src):
        src = 'None'
    if type(dest) != str and np.isnan(dest):
        dest = 'None'
    if src not in setOfsets[0]:
        setOfsets[0][src] = 1
    else:
        setOfsets[0][src] += 1
    if dest not in setOfsets[1]:
        setOfsets[1][dest] = 1
    else:
        setOfsets[1][dest] += 1


def seperate(df):
    gb = df.groupby('graphId')
    graphs = [gb.get_group(x) for x in gb.groups]
    return graphs


def readPandasFile(parsedFile, names=(
"sourceId", "sourceType", "destinationId", "destinationType", "syscal", "retTime", "graphId", "freq", "spid"), sep=','):
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


def getFreqScore(src, dest, syscal, freqDict, max_freq):
    srcRel = (src, syscal)
    if srcRel not in freqDict:
        return 1 / max_freq
    if dest not in freqDict[srcRel]:
        return 1 / max_freq
    return freqDict[srcRel][dest] / freqDict[srcRel]['total']


def toList(df):
    return df.values.tolist()

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


def is_valid_ip(ip):
    ip_regex = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    if re.fullmatch(ip_regex, ip):
        parts = ip.split('.')
        return all(0 <= int(part) <= 255 for part in parts)
    return False


def get_token_count(setOfsets):
    nodes = list(setOfsets[0].keys()) + list(setOfsets[1].keys())
    nodes = set(nodes)
    sum_sets = {}
    sum_token = 0
    count = 0

    for p in list(nodes):
        if is_valid_ip(p):
            continue
        if p in setOfsets[0] and p in setOfsets[1]:
            sum_sets[p] = setOfsets[0][p] + setOfsets[1][p]
        elif p in setOfsets[0] and p not in setOfsets[1]:
            sum_sets[p] = setOfsets[0][p]
        else:
            sum_sets[p] = setOfsets[1][p]

    avg_count = sum(sum_sets.values()) // len(sum_sets)

    for k in sum_sets.keys():
        if sum_sets[k] > avg_count:
            w = k.split('/')
            sum_token += len(w) - 1
            count += 1

    return sum_token // count


def addToAdjList(s, d, edge, adjListForward, adjListBackward):
    adjListForward.setdefault(s, [])
    adjListForward[s].append((edge[0], d, edge[1], edge[2]))

    adjListBackward.setdefault(d, [])
    adjListBackward[d].append((edge[0], s, edge[1], edge[2]))


def createAdjListCleanly(parsedList, setOfsets, freqDict, max_freq, id_map, model, path_vectors, avg_tokens):
    adjListForward = {}
    adjListBackward = {}
    anomalous_node = set()
    anomalous_name = set()
    common_name = set()
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

        src_id = row[0]
        dest_id = row[2]
        syscal = row[4]
        src_name = id_map['name_map'][src_id]
        dest_name = id_map['name_map'][dest_id]
        src_type = row[1]
        dest_type = row[3]

        inScore = getInScore(src_name, setOfsets)
        outScore = getOutScore(dest_name, setOfsets)
        freqScore = getFreqScore(src_name, dest_name, syscal, freqDict, max_freq)

        if outScore == 0:
            if dest_name not in common_name:
                if dest_name not in anomalous_name:
                    if dest_type != 'netflow':
                        new_path_vector = get_path_vector(dest_name, model)
                        new_path_vector = normalize([new_path_vector], norm='l2')[0]
                        cos_similarities = cosine_similarity([new_path_vector], path_vectors)[0]
                        average_similarity = np.mean(cos_similarities)

                        if average_similarity < 0.35 and len(dest_name.split('/')) - 1 < avg_tokens:
                            anomalous_node.add((row[2], row[3]))
                            anomalous_name.add(dest_name)
                        else:
                            common_name.add(dest_name)
                    else:
                        anomalous_node.add((row[2], row[3]))

                else:
                    anomalous_node.add((row[2], row[3]))

            outScore = 1 / sum(setOfsets[1].values())

        if inScore == 0:
            if src_name not in common_name:
                if src_name not in anomalous_name:
                    if src_type != 'netflow':
                        new_path_vector = get_path_vector(src_name, model)
                        new_path_vector = normalize([new_path_vector], norm='l2')[0]
                        cos_similarities = cosine_similarity([new_path_vector], path_vectors)[0]
                        average_similarity = np.mean(cos_similarities)
                        if average_similarity < 0.35 and len(src_name.split('/')) - 1 < avg_tokens:
                            anomalous_node.add((row[0], row[1]))
                            anomalous_name.add(src_name)
                        else:
                            common_name.add(src_name)
                    else:
                        anomalous_node.add((row[0], row[1]))

                else:
                    anomalous_node.add((row[0], row[1]))

            inScore = 1 / sum(setOfsets[0].values())

        score = math.log2(inScore * freqScore * outScore) * -1

        addToAdjList(src, dest, (row[5], row[4], score), adjListForward, adjListBackward)

    return adjListForward, adjListBackward, list(anomalous_node)



def find_key_nodes(node2path):
    nodes = set()
    for k, paths in node2path.items():
        for p in paths:
            for i in range(len(p)):
                if i % 2 == 0:
                    nodes.add(p[i])
    return nodes


def makeAdjListDAGFaster(adjListForward):
    forwardEdges = []
    setOfNodes = {}
    dagForAdj = {}
    dagDestAdj = {}
    for src in adjListForward:
        for edge in adjListForward[src]:
            forwardEdges.append((edge[0], src, edge[1], edge[2], edge[3]))  # (retTime, src, dst, reltype, score)
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


def backward_find_k1(adj, node, depth):
    queue = deque([(node, 0, 0, [node], [0], [])])
    visited = set([(node, 0, 0)])
    paths = []
    scores = []
    times = []
    events = []

    while queue:
        current_node, current_time, current_score, path, time, event = queue.popleft()
        neighbors = adj.get(current_node, [])

        if not neighbors:
            scores.append(current_score)
            paths.append(path)
            times.append(time)
            events.append(event)
            continue

        for neighbor, attrs in neighbors:
            next_time, event_type, score = attrs
            state = (neighbor, next_time, event_type)

            if state not in visited:
                if current_time == 0 or next_time <= current_time:
                    visited.add(state)
                    new_path = path + [neighbor]
                    new_time = time + [next_time]
                    new_event = event + [event_type]
                    new_score = current_score + score

                    if len(new_path) - 1 == depth:
                        if new_path not in paths:
                            scores.append(new_score)
                            paths.append(new_path)
                            times.append(new_time)
                            events.append(new_event)
                    else:
                        queue.append((neighbor, next_time, new_score, new_path, new_time, new_event))

    return paths, scores, times, events


def forword_find_k1(adj, node, depth):
    queue = deque([(node, 0, 0, [node], [0], [])])
    visited = set([(node, 0, 0)])
    paths = []
    scores = []
    times = []
    events = []

    while queue:
        current_node, current_time, current_score, path, time, event = queue.popleft()
        neighbors = adj.get(current_node, [])

        if not neighbors:
            scores.append(current_score)
            paths.append(path)
            times.append(time)
            events.append(event)
            continue

        for neighbor, attrs in neighbors:
            next_time, event_type, score = attrs
            state = (neighbor, next_time, event_type)

            if state not in visited:
                if current_time == 0 or next_time >= current_time:
                    visited.add(state)
                    new_path = path + [neighbor]
                    new_time = time + [next_time]
                    new_event = event + [event_type]
                    new_score = current_score + score

                    if len(new_path) - 1 == depth:
                        if new_path not in paths:
                            scores.append(new_score)
                            paths.append(new_path)
                            times.append(new_time)
                            events.append(new_event)
                    else:
                        queue.append((neighbor, next_time, new_score, new_path, new_time, new_event))

    return paths, scores, times, events


def findKAnomlousPaths(forAdj, backAdj, k1, k2, anomalous_node, graphName, id_map, path2score):

    anomalous_all_paths = {}
    anomalous_all_paths2index = {}
    anomalous_all_paths2itime = {}

    for i in tqdm(range(0, len(anomalous_node))):
        a_n = anomalous_node[i]
        a_n_anomalous_paths = {}
        a_n_anomalous_edges = {}
        a_n_anomalous_score = {}
        a_n_anomalous_times = {}

        if a_n[1] == 'subject':
            src = (a_n[0], graphName, a_n[1], 0)
        else:
            src = (a_n[0], a_n[1], 0)

        if src in forAdj:
            forward_paths, forward_scores, forward_times, forward_events = forword_find_k1(forAdj, src, k1)
        else:
            forward_paths, forward_scores, forward_times, forward_events = [], [], [], []
        if src in backAdj:
            backward_paths, backward_scores, backward_times, backward_events = backward_find_k1(backAdj, src, k1)
        else:
            backward_paths, backward_scores, backward_times, backward_events = [], [], [], []

        if len(forward_paths) != 0 and len(backward_paths) == 0:
            for i in range(len(forward_scores)):
                a_n_anomalous_score[i] = forward_scores[i]
                a_n_anomalous_paths[i] = forward_paths[i]
                a_n_anomalous_edges[i] = forward_events[i]
                a_n_anomalous_times[i] = forward_times[i]


        elif len(forward_paths) == 0 and len(backward_paths) != 0:
            for i in range(len(backward_scores)):
                a_n_anomalous_score[i] = backward_scores[i]
                a_n_anomalous_paths[i] = backward_paths[i][::-1]
                a_n_anomalous_edges[i] = backward_events[i][::-1]
                a_n_anomalous_times[i] = backward_times[i][::-1]

        else:
            for_start_time = [t[1] for t in forward_times]
            back_start_time = [t[1] for t in backward_times]

            for i in range(len(back_start_time)):
                for j in range(len(for_start_time)):
                    if back_start_time[i] < for_start_time[j]:
                        a_n_anomalous_score[i] = forward_scores[j] + backward_scores[i]
                        a_n_anomalous_paths[i] = backward_paths[i][::-1] + forward_paths[j][1:]
                        a_n_anomalous_edges[i] = backward_events[i][::-1] + forward_events[j]
                        a_n_anomalous_times[i] = backward_times[i][::-1] + forward_times[j]

        sort_scores = dict(sorted(a_n_anomalous_score.items(), reverse=True, key=lambda item: item[1])).keys()
        sort_scores = list(sort_scores)
        src_id = id_map['name_map'][a_n[0]]

        anomalous_all_paths[(a_n[0], src_id)] = []
        anomalous_all_paths2index[(a_n[0], src_id)] = []
        anomalous_all_paths2itime[(a_n[0], src_id)] = []
        for s in sort_scores:
            if len(anomalous_all_paths[(a_n[0], src_id)]) <= k2:
                path = []
                path2index = []
                edge2time = []
                count = 0
                for j in range(len(a_n_anomalous_paths[s])):
                    dst_id = id_map['name_map'][a_n_anomalous_paths[s][j][0]]
                    path.append(dst_id)
                    path2index.append(a_n_anomalous_paths[s][j][0])
                    if count != len(a_n_anomalous_paths[s]) - 1:
                        path.append(a_n_anomalous_edges[s][j])
                        path2index.append(a_n_anomalous_edges[s][j])
                        edge2time.append(a_n_anomalous_times[s][j])
                    count += 1
                if path not in anomalous_all_paths[(a_n[0], src_id)]:
                    anomalous_all_paths[(a_n[0], src_id)].append(path)
                    anomalous_all_paths2index[(a_n[0], src_id)].append(path2index)
                    anomalous_all_paths2itime[(a_n[0], src_id)].append(edge2time)
                    path2score[tuple(path)] = a_n_anomalous_score[s]
            else:
                break

    return anomalous_all_paths, path2score, anomalous_all_paths2index, anomalous_all_paths2itime


def findnetflownode(sum_paths, id_map, final_paths):
    net_map = {}
    record = {}
    for k, paths in sum_paths.items():
        if id_map['type_map'][k[0]] == 'netflow':
            if k[1] not in net_map:
                net_map[k[1]] = paths
            else:
                for p in paths:
                    if p not in net_map[k[1]]:
                        net_map[k[1]].append(p)
            if k[1] not in record:
                record[k[1]] = [k]
            else:
                record[k[1]].append(k)

    for k, paths in sum_paths.items():
        if id_map['type_map'][k[0]] == 'netflow':
            if k == record[k[1]][0]:
                final_paths[k] = net_map[k[1]]
        else:
            final_paths[k] = paths

    return final_paths


