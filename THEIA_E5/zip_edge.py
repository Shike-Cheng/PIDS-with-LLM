from utils import *
from config import *
import json
import csv


def read(path):
    data = json.load(open(path, 'r'))
    return data

dataflow = [
    "EVENT_WRITE",
    "EVENT_READ",
    "EVENT_MMAP",
    "EVENT_SENDTO",
    "EVENT_RECVFROM"
]

def judge_merge(time_map, s, t1, t2):
    if s in time_map:
        if time_map[s] > t1 and time_map[s]< t2:
                return True
    return False


def compress_events(begin, end):
    new_msg_map = {}
    dataflow_map = {}
    for d in range(begin, end + 1):
        data = json.load(open(artifact_path + "graph_5_" + str(d) + ".json", "r"))

        for i in tqdm(range(len(data))):
            e = data[i]
            if e[2] == "EVENT_OPEN":
                continue
            if e[2] in dataflow:
                if int(e[4]) not in dataflow_map:
                    dataflow_map[int(e[4])] = int(e[5])
                else:
                    if dataflow_map[int(e[4])] < int(e[5]):
                        dataflow_map[int(e[4])] = int(e[5])

            str_dst = str((int(e[1]), int(e[4])))
            if str_dst not in new_msg_map:
                new_msg_map[str_dst] = {}

            if e[2] not in new_msg_map[str_dst]:
                new_msg_map[str_dst][e[2]] = [{'time': int(e[5]), 'index': int(e[6]), 'count': 1}]

            else:
                if judge_merge(dataflow_map, int(e[1]), new_msg_map[str_dst][e[2]][-1]['time'], int(e[5])):
                    new_msg_map[str_dst][e[2]].append({'time': int(e[5]), 'index': int(e[6]), 'count': 1})
                else:
                    new_msg_map[str_dst][e[2]][-1]['count'] += 1

    f_learn = open(artifact_path + 'compress_graph_' + str(begin) + '_' + str(end) + '.json', 'w', encoding='utf-8')
    json.dump(new_msg_map, f_learn)

    new_edges_list = []

    for k, v in new_msg_map.items():
        src = eval(k)[0]
        dst = eval(k)[1]
        for t, edges in v.items():
            for e in edges:
                if t == "EVENT_WRITE" and node_msg['type_map'][str(dst)] == "netflow":
                    t = "EVENT_SENDTO"
                if t == "EVENT_READ" and node_msg['type_map'][str(src)] == "netflow":
                    t = "EVENT_RECVFROM"
                new_edges_list.append(
                    [src, node_msg['type_map'][str(src)], dst, node_msg['type_map'][str(dst)], t, e['time'], 0,
                     e['count'], 0])

    new_edges_list = sorted(new_edges_list, key=lambda x: x[5])

    with open(artifact_path + 'day_' + str(begin) + '_' + str(end) + '.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in new_edges_list:
            writer.writerow(row)

if __name__ == "__main__":
    node_msg = json.load(open(artifact_path + database + '_map' + '.json', 'r', encoding='utf-8'))
    # 8, 9
    compress_events(8, 9)
    # 15
    compress_events(15, 15)

