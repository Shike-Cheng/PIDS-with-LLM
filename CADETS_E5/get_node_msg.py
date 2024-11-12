from config import *
from tqdm import tqdm
from utils import *
import json
import re

def extract_ip_address(text):
    pattern = r'(?<=->)((25[0-5]|2[0-4]\d|1?\d{1,2})\.){3}(25[0-5]|2[0-4]\d|1?\d{1,2})'
    match = re.search(pattern, text)
    if match:
        return match.group(0)
    return None


def gen_nodeid2msg(cur):
    id2uuid = {}
    id2name = {}
    id2type = {}
    sql = "select * from node2id ORDER BY index_id;"
    cur.execute(sql)
    rows = cur.fetchall()

    for j in tqdm(range(len(rows))):
        i = rows[j]
        id2uuid[i[-1]] = i[0]
        id2type[i[-1]] = i[1]
        if i[2] == None:
            print(i[2])

        if '->NA:0' in i[2] or '->NETLINK:0' in i[2]:
            id2name[i[-1]] = "127.0.0.1"
        elif '->' in i[2]:
            id2name[i[-1]] = extract_ip_address(i[2])
        else:
            id2name[i[-1]] = i[2]

        if id2name[i[-1]] == None:
            print(id2name[i[-1]], i[2])

    return id2name, id2type,  id2uuid

f_map = open(artifact_path + database + '_map' + '.json', 'w', encoding='utf-8')
cur, _ = init_database_connection()
    
sum_map = {}
sum_map['name_map'], sum_map['type_map'], sum_map['uuid_map'] = gen_nodeid2msg(cur)
json.dump(sum_map, f_map)