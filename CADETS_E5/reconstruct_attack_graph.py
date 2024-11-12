import ast
import networkx as nx
import csv
import json
import time
from config import *

key_words = ['/etc/passwd', 'hostname']

def readFromFile(filename):
    with open(filename, 'r') as file:
        data_str = file.read()
        data = ast.literal_eval(data_str)
        return data

def get_neighbors(node2r, node2p):

    node2neighbors = {}
    for k, r in node2r.items():
        if r == 'malicious' or r == 'Malicious':
            if k not in node2neighbors:
                node2neighbors[k] = set()
            for p in node2p[k]:
                for i in range(len(p)):
                    if i % 2 == 0:
                        node2neighbors[k].add(p[i])

    return node2neighbors

def merge_sets(sets):
    merged = []
    visited = [False] * len(sets)

    def dfs(index, current_set):
        visited[index] = True
        current_set.update(sets[index])
        for i in range(len(sets)):
            if not visited[i] and not current_set.isdisjoint(sets[i]):
                dfs(i, current_set)

    for i in range(len(sets)):
        if not visited[i]:
            current_set = set()
            dfs(i, current_set)
            merged.append(current_set)

    return merged

def get_path_neighbors(node2n, paths2score, node2paths, paths2index):
    node2count = {}
    for k in paths2index.keys():
        if nodemap["type_map"][k[0]] == 'netflow':
            if k[1] not in node2count:
                node2count[k[1]] = 1
            else:
                node2count[k[1]] += 1

    node2clusters = {}
    malicious_nodes = list(node2n.keys())
    for n in malicious_nodes:
        if n not in node2clusters:
            node2clusters[n] = {n}
        for m in malicious_nodes:
            if n != m:
                for nn in key_words:
                    if nn in node2n[n] and nn in node2n[m]:
                        node2clusters[n].add(m)
                        continue
                for s in node2n[n]:
                    if m[1] in s:
                        node2clusters[n].add(m)

    set_list = list(node2clusters.values())
    clusters = merge_sets(set_list)
    print("clusters", clusters)

    max_len = max(len(sublist) for sublist in clusters)

    if max_len == 1 and len(clusters) < 5:
        new_clusters = set()
        for c in clusters:
            new_clusters = new_clusters | c
        key_clusters = new_clusters
    else:
        id2score = {}

        for i in range(len(clusters)):
            count = 0
            score = 0
            if len(clusters[i]) == 1:
                continue
            for n in clusters[i]:
                if n[1] in node2count:
                    count += node2count[n[1]]
                else:
                    count += 1
                for p in node2paths[n]:
                    score += paths2score[tuple(p)]

            id2score[i] = score / count
        print(id2score)
        key_clusters = clusters[min(id2score, key=id2score.get)]


    return key_clusters


def writeToFile(data, filename):
    with open(filename, 'w') as file:
        file.write(str(data))

def read(path):
    data = json.load(open(path, 'r'))
    return data

def judge_has_edge(node, nodes_clusters, G):
    flag = False
    for n in list(nodes_clusters):
        if G.has_edge(node, n):
            flag = True
            break

    return flag

def find_xiyou_node_neighbors(mal_nodes, paths2index, knowledge_string_database, G):
    mal_nodes_name = set()
    for m in list(mal_nodes):
        mal_nodes_name.add(m[1])

    all_mal_nodes = set()
    node2nebor = {}
    for k, paths in paths2index.items():
        if k[1] in mal_nodes_name and k not in mal_nodes and nodemap["type_map"][k[0]] == "netflow":
            mal_nodes.add(k)
        if k not in node2nebor:
            node2nebor[k] = set()
        for p in paths:
            for i in range(len(p)):
                if i % 2 == 0 and p[i] != k[0]:
                    node2nebor[k].add(p[i])

    print(mal_nodes)
    for n in list(mal_nodes):
        if nodemap["type_map"][n[0]] != 'netflow':
            all_mal_nodes.add(n[0])
            all_mal_nodes = all_mal_nodes | node2nebor[n]
            print(n, node2nebor[n])
            if n[1] not in knowledge_string_database[0].keys() and n[1] not in knowledge_string_database[1].keys() and n[1] not in ['nginx']:
                for kk in list(G.neighbors(n[0])):
                    if judge_has_edge(kk, all_mal_nodes, G) and nodemap["name_map"][kk] != '127.0.0.1':
                        print(n, kk, nodemap["name_map"][kk])
                        all_mal_nodes.add(kk)

    for n in list(all_mal_nodes):
        print(n, nodemap["name_map"][n])

    if len(all_mal_nodes) == 0:
        for n in list(mal_nodes):
            all_mal_nodes.add(n[0])
            all_mal_nodes = all_mal_nodes | node2nebor[n]
    else:
        new_netflow_related = set()
        for n in list(mal_nodes):
            if nodemap["type_map"][n[0]] == 'netflow':
                flag = False
                for nn in G.neighbors(n[0]):
                    for nnn in G.neighbors(nn):
                        if nodemap["name_map"][nnn] in key_words:
                            new_netflow_related.add(nnn)
                            flag = True
                            break
                    if flag or nn in all_mal_nodes:
                        new_netflow_related.add(n[0])
                        new_netflow_related = new_netflow_related | set(G.neighbors(n[0]))
                        break

        all_mal_nodes = all_mal_nodes | new_netflow_related

    uuid_list = []
    uuid2map = {}
    id2map = {}

    for n in list(all_mal_nodes):
        print(n, nodemap["name_map"][n])
        uuid_list.append(nodemap["uuid_map"][n])
        uuid2map[nodemap["uuid_map"][n]] = nodemap["name_map"][n]
        id2map[n] = nodemap["name_map"][n]

    return uuid2map, id2map


if __name__=='__main__':
    knowledge_string_database = readFromFile(artifact_path + "day_8_11_setOfsets.data")
    node2result = readFromFile(artifact_path + "day_17_result.data")
    node2paths = readFromFile(artifact_path + "day_17_17_anomalous_path.data")
    paths2index = readFromFile(artifact_path + "day_17_17_anomalous_path2index.data")
    paths2score = readFromFile(artifact_path + "day_17_17_anomalous_path2score.data")
    nodemap = read(artifact_path + database + '_map' + '.json')
    edges = set()
    start_time = time.time()
    with open(artifact_path + 'day_17_17.csv', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            edges.add((row[0], row[2]))
    G = nx.Graph()
    G.add_edges_from(list(edges))

    node2neighbors = get_neighbors(node2result, node2paths)
    mal_nodes = get_path_neighbors(node2neighbors, paths2score, node2paths, paths2index)
    all_malicious_nodes_uuid2msg, all_malicious_nodes_id2msg = find_xiyou_node_neighbors(mal_nodes, paths2index, knowledge_string_database, G)
    
    writeToFile(all_malicious_nodes_uuid2msg, artifact_path + "day_17_malicious_nodes_uuid2msg.data")
    writeToFile(all_malicious_nodes_id2msg, artifact_path + "day_17_malicious_nodes_id2msg.data")


