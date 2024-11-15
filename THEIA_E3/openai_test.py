import openai
import ast
from gensim.models.doc2vec import Doc2Vec
import pandas as pd
import re
import numpy as np
import csv
import logging
from config import *


model = Doc2Vec.load(artifact_path + "day_3_5_path.model")
openai.api_key = ""

relMap = {
    "EVENT_WRITE" : "WR",
    "EVENT_READ" : "RD",
    "EVENT_MMAP" : "MP",
    "EVENT_SENDTO" : "ST",
    "EVENT_RECVFROM" : "RF",
    "EVENT_EXECUTE" : "EX",
    "EVENT_FORK" : "FR"
}
def readFromFile(filename):
    with open(filename, 'r') as file:
        data_str = file.read()
        data = ast.literal_eval(data_str)
        return data

def writeToFile(data, filename):
    with open(filename, 'w') as file:
        file.write(str(data))

def readFromcsv(filename):
    data = []
    with open(filename, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if len(row) == 3:
                data.append([eval(row[0]), row[1], eval(row[2])])
            else:
                data.append([eval(row[0]), row[1], []])
    return data

def get_sentence_embedding(sentence):
    return model.infer_vector(sentence.split())

def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    similarity = dot_product / (norm_vec1 * norm_vec2)
    return similarity

def parse_embedding(embedding_str):
    return np.array([float(x) for x in embedding_str.split(',')])


def chat(prompt, context):
    response = openai.ChatCompletion.create(
        model=""
              "",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": context}
        ]
    )
    answer = response.choices[0].message['content']
    return answer

def cal_similar(path):
    path_embedding = get_sentence_embedding(path)
    score_list = []
    for i in range(len(embeddings)):
        score = cosine_similarity(embeddings[i], path_embedding)
        score_list.append(score)

    score_array = np.array(score_list)
    sorted_indices = np.argsort(score_array)
    top_5_indices = sorted_indices[-1:][::-1]

    return top_5_indices

def make_path_prompt(top_index):
    prompt_sents = set()
    for i in top_index:
        for j in range(len(database_data[i][0])):
            if database_data[i][0][j] in relMap:
                database_data[i][0][j] = relMap[database_data[i][0][j]]
        if len(database_data[i][2]) > 1:
            prompt_sents.add(str(database_data[i][0]) + " is malicious where the malicious nodes are " + str(database_data[i][2]))
        elif len(database_data[i][2]) == 1:
            prompt_sents.add(str(database_data[i][0]) + " is malicious where the malicious node is " + str(database_data[i][2]))
        else:
            prompt_sents.add(str(database_data[i][0]) + " is benign")

    return prompt_sents

def make_context(node, paths, n_result):
    node_name = node[1]

    p = """Now the underlying system has a node '""" + node_name + """' which is involved in the following system interactions:
"""
    for pp in paths:
        p += str(pp) + '\n'

    p += """Based on the above information please help me determine if the node '""" + node_name + """' is benign or malicious. Remember, most of the nodes are benign nodes generated by the benign activity of the system, and that there are very few malicious nodes. You can't conclude an node (files/process/netflow) is malicious without sufficient evidence. You just need to answer me malicious or benign, not an explanation."""

    return p

def make_prompt(path_prompt, epoch):

    c = """A cyberattack event has occurred on a current host, and you are a network security expert who can determine whether target node is malicious or benign based on the log information (path information). Event abbreviations are mapped as: {"EVENT_WRITE": "WR", "EVENT_READ": "RD", "EVENT_MMAP": "MP", "EVENT_SENDTO": "ST", "EVENT_RECVFROM": "RF", "EVENT_EXECUTE": "EX", "EVENT_FORK": "FR"}.
Here are some path-level event messages on this host for you to make a sound judgment: """

    for p in path_prompt:
        c += '\n' + p
    return c


def make_list_of_sets(count, paths):
    list_of_sets = [set() for _ in range(count)]
    for p in paths:
        for i in range(len(p)):
            list_of_sets[i].add(p[i])

    return list_of_sets

def merge_paths(sum_paths):

    length_map = {}
    for p in sum_paths:
        if len(p) not in length_map:
            length_map[len(p)] = []
        length_map[len(p)].append(p)

    new_paths = []
    for k, paths in length_map.items():
        group, ungroup = group_lists_by_unique_diff(k, paths)
        for ps in group:
            new_path = []
            node_set_list = make_list_of_sets(k, ps)
            for s in node_set_list:
                if len(s) > 1:
                    nodes_str = ""
                    nodes = list(s)
                    for i in range(len(nodes)):
                        if i != len(nodes) - 1:
                            nodes_str += nodes[i] + ', '
                        else:
                            nodes_str += nodes[i]
                    new_path += [nodes_str]
                else:
                    new_path += list(s)
            new_paths.append(new_path)

        for p in ungroup:
            new_paths.append(p)

    new_paths = zip_tokens(new_paths)

    return new_paths

def judge(k, paths):
    list_sets = make_list_of_sets(k, paths)
    c = 0
    common_string = ""
    nodes_index = -1
    for i in range(len(list_sets)):
        if len(list_sets[i]) == 1:
            common_string += list(list_sets[i])[0]
        else:
            c += 1
            nodes_index = i

    return c, common_string, nodes_index

def keep_longest_list_with_target(t_k, string_map, target):
    nested_list = []
    for ss in t_k:
        nested_list.append(string_map[ss])

    longest_list = None
    for lst in nested_list:
        if target in lst:
            if longest_list is None or len(lst) > len(longest_list):
                longest_list = lst

    result = []
    for lst in nested_list:
        if target in lst:
            if lst is longest_list:
                result.append(lst)
            else:
                result.append([x for x in lst if x != target])
        else:
            result.append(lst)

    for i in range(len(t_k)):
        string_map[t_k[i]] = result[i]

    return string_map

def group_lists_by_unique_diff(length, lists):

    string2paths_map = {}
    string2node_index_map = {}
    for i in range(len(lists)):
        target_n = set()
        for j in range(i + 1, len(lists)):
            f, s, n_index = judge(length, [lists[i], lists[j]])
            if f == 1:
                target_n.add(s)
                if s not in string2paths_map:
                    string2paths_map[s] = []
                if lists[i] not in string2paths_map[s]:
                    string2paths_map[s].append(lists[i])
                if lists[j] not in string2paths_map[s]: string2paths_map[s].append(lists[j])
                if s not in string2node_index_map: string2node_index_map[s] = n_index
        string2paths_map = keep_longest_list_with_target(list(target_n), string2paths_map, lists[i])

    grouped = []
    ungrouped = []
    record_path = []
    for s, ps in string2paths_map.items():
        if len(ps) == 1:
            continue
        if string2node_index_map[s] % 2 == 0:
            record_path += ps
            grouped.append(ps)
    for p in lists:
        if p not in record_path:
            ungrouped.append(p)

    return grouped, ungrouped


def zip_tokens(paths):
    for p in paths:
        for i in range(len(p)):
            if p[i] in relMap:
                p[i] = relMap[p[i]]

    return paths

def is_valid_ip(ip):
    pattern = r'^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    return re.match(pattern, ip) is not None


def Adjustment_detection_sequence(data):
    new_data = {}
    netflow_data = {}
    for k, ps in data.items():
        if is_valid_ip(k[1]):
            netflow_data[k] = ps
        else:
            new_data[k] = ps

    new_data.update(netflow_data)
    return new_data


if __name__=='__main__':

    anomalous_data = readFromFile(artifact_path + "day_10_10_anomalous_path.data")
    database_data = readFromcsv(artifact_path + "day_3_5_path.csv")
    embeddings = np.load(artifact_path + 'day_3_5_path.npy')
    result = {}
    epoch = 1
    anomalous_data = Adjustment_detection_sequence(anomalous_data)
    for k, paths in anomalous_data.items():
        if len(paths) != 0:
            path_prompts = set()
            for p in paths:
                s = ""
                count = 0
                for n in p:
                    if count == len(p) - 1: s += n
                    else: s += n + ' '
                    count += 1
                top_path = cal_similar(s)
                path_prompts = path_prompts.union(make_path_prompt(top_path))
            zip_path = merge_paths(paths)
            prompt = make_prompt(path_prompts, epoch)
            context = make_context(k, zip_path, result)

            print(prompt)
            print(context)
            response = chat(prompt, context)
            result[k] = response
            epoch += 1
            print('\n')
            print(k[1] + "Result****: ", result[k])

    writeToFile(result, artifact_path + "day_10_result.data")
