import os
import re
import torch
from tqdm import tqdm
import hashlib

from config import *
from utils import *

filelist = ['ta1-theia-e3-official-6r.json.9',
 'ta1-theia-e3-official-1r.json.9',
 'ta1-theia-e3-official-6r.json.8',
 'ta1-theia-e3-official-6r.json.12',
 'ta1-theia-e3-official-1r.json.7',
 'ta1-theia-e3-official-6r.json.7',
 'ta1-theia-e3-official-6r.json.5',
 'ta1-theia-e3-official-1r.json.3',
 'ta1-theia-e3-official-6r.json',
 'ta1-theia-e3-official-1r.json.5',
 'ta1-theia-e3-official-6r.json.11',
 'ta1-theia-e3-official-1r.json.4',
 'ta1-theia-e3-official-1r.json.6',
 'ta1-theia-e3-official-5m.json',
 'ta1-theia-e3-official-1r.json.2',
 'ta1-theia-e3-official-6r.json.10',
 'ta1-theia-e3-official-6r.json.4',
 'ta1-theia-e3-official-6r.json.1',
 'ta1-theia-e3-official-3.json',
 'ta1-theia-e3-official-1r.json.8',
 'ta1-theia-e3-official-1r.json.1',
 'ta1-theia-e3-official-6r.json.6',
 'ta1-theia-e3-official-6r.json.2',
 'ta1-theia-e3-official-6r.json.3',
 'ta1-theia-e3-official-1r.json']


def store_netflow(file_path, cur, connect):

    netobj2hash = {}
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                #                 pass
                if '{"datum":{"com.bbn.tc.schema.avro.cdm18.NetFlowObject"' in line:

                    try:
                        res = re.findall(
                            'NetFlowObject":{"uuid":"(.*?)"(.*?)"localAddress":"(.*?)","localPort":(.*?),"remoteAddress":"(.*?)","remotePort":(.*?),',
                            line)[0]

                        nodeid = res[0]
                        srcaddr = res[2]
                        srcport = res[3]
                        dstaddr = res[4]
                        dstport = res[5]

                        nodeproperty = srcaddr + "," + srcport + "," + dstaddr + "," + dstport
                        netobj2hash[nodeid] = [nodeproperty]
                    except:

                        pass

    # Store data into database
    datalist = []
    for i in netobj2hash.keys():
        datalist.append([i] + netobj2hash[i][0].split(","))

    sql = '''insert into netflow_node_table
                         values %s
            '''

    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()

def store_subject(file_path, cur, connect):

    subject_obj2hash = {}
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if '{"datum":{"com.bbn.tc.schema.avro.cdm18.Subject"' in line:
                    res = re.findall(
                        'Subject":{"uuid":"(.*?)"(.*?)"cmdLine":{"string":"(.*?)"}(.*?)"properties":{"map":{"tgid":"(.*?)"',line)[0]
                    try:
                        path_str = re.findall('"path":"(.*?)"', line)[0]
                        path = path_str
                    except:
                        path = "null"
                    nodeid = res[0]
                    cmdLine = res[2]
                    tgid = res[4]

                    subject_obj2hash[nodeid] = [cmdLine, tgid, path]

    # Store into database
    datalist = []
    for i in subject_obj2hash.keys():
        datalist.append([i] + subject_obj2hash[i])
    sql = '''insert into subject_node_table
                         values %s
            '''
    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()


def store_file(file_path, cur, connect):


    file2hash = {}
    nullcount = 0

    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if '{"datum":{"com.bbn.tc.schema.avro.cdm18.FileObject"' in line:
                    try:

                        res = re.findall('FileObject":{"uuid":"(.*?)"(.*?)"filename":"(.*?)"', line)[0]
                        nodeid = res[0]
                        filepath = res[2]
                        file2hash[nodeid] = filepath

                    except:
                        pass
    datalist = []
    for i in file2hash.keys():
        datalist.append([i] + [file2hash[i]])

    sql = '''insert into file_node_table
                         values %s
            '''
    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()

def create_node_list(cur, connect):
    node_list = {}

    # file
    sql = """
    select * from file_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()

    for i in records:
        node_list[i[0]] = ["file", i[-1]]
    file_uuidList = []
    for i in records:
        file_uuidList.append(i[0])

    # subject
    sql = """
    select * from subject_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()
    for i in records:
        node_list[i[0]] = ["subject", i[-1]]
    subject_uuidList = []
    for i in records:
        subject_uuidList.append(i[0])

    # netflow
    sql = """
    select * from netflow_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()
    for i in records:
        node_list[i[0]] = ["netflow", i[-4] + ":" + i[-3] + "->" + i[-2] + ":" + i[-1]]
    netflow_uuidList = []
    for i in records:
        netflow_uuidList.append(i[0])

    node_list_database = []
    node_index = 0
    for i in node_list:
        node_list_database.append([i] + node_list[i] + [node_index])
        node_index += 1

    sql = '''insert into node2id
                         values %s
            '''
    ex.execute_values(cur, sql, node_list_database, page_size=10000)
    connect.commit()

    sql = "select * from node2id ORDER BY index_id;"
    cur.execute(sql)
    rows = cur.fetchall()
    nodeid2msg = {}
    for i in rows:
        nodeid2msg[i[0]] = i[-1]  # uuid_id -> index_id
    return nodeid2msg, subject_uuidList, file_uuidList, netflow_uuidList

def store_event(file_path, cur, connect, reverse, nodeid2msg, subject_uuid2hash, file_uuid2hash, net_uuid2hash):
    valid_subjects = set(subject_uuid2hash)
    valid_allnodes = set(subject_uuid2hash + file_uuid2hash + net_uuid2hash)
    # relMapKeys = set(relMap.keys())

    subject_uuid_pattern = re.compile('"subject":{"com.bbn.tc.schema.avro.cdm18.UUID":"(.*?)"}')
    predicateObject_uuid_pattern = re.compile('"predicateObject":{"com.bbn.tc.schema.avro.cdm18.UUID":"(.*?)"}')
    type_pattern = re.compile('"type":"(.*?)"')
    timestamp_pattern = re.compile('"timestampNanos":(.*?),')

    datalist = []
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if '{"datum":{"com.bbn.tc.schema.avro.cdm18.Event"' in line:
                    relation_type_match = type_pattern.search(line)
                    if relation_type_match:
                        relation_type = relation_type_match.group(1)
                        subject_uuid_match = subject_uuid_pattern.search(line)
                        predicateObject_uuid_match = predicateObject_uuid_pattern.search(line)
                        if subject_uuid_match and predicateObject_uuid_match:
                            subject_uuid = subject_uuid_match.group(1)
                            predicateObject_uuid = predicateObject_uuid_match.group(1)
                            if subject_uuid in valid_subjects and predicateObject_uuid in valid_allnodes:
                                time_rec_match = timestamp_pattern.search(line)
                                if time_rec_match:
                                    time_rec = int(time_rec_match.group(1))
                                    subjectId = subject_uuid
                                    objectId = predicateObject_uuid
                                    if relation_type in reverse:
                                        datalist.append(
                                            [objectId, nodeid2msg[objectId], relation_type, subjectId, nodeid2msg[subjectId],
                                            time_rec])
                                    else :
                                        datalist.append(
                                            [subjectId, nodeid2msg[subjectId], relation_type, objectId, nodeid2msg[objectId],
                                            time_rec])

    sql = '''insert into event_table
                         values %s
            '''
    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()


if __name__ == "__main__":
    cur, connect = init_database_connection()

    print("Processing netflow data")
    store_netflow(file_path=raw_dir, cur=cur, connect=connect)

    print("Processing subject data")
    store_subject(file_path=raw_dir, cur=cur, connect=connect)

    print("Processing file data")
    store_file(file_path=raw_dir, cur=cur, connect=connect)

    print("Extracting the node list")
    nodeid2msg, subject_uuidList, file_uuidList, netflow_uuidList = create_node_list(cur=cur, connect=connect)

    print("Processing the events")
    store_event(
        file_path=raw_dir,
        cur=cur,
        connect=connect,
        reverse=edge_reversed,
        nodeid2msg=nodeid2msg,
        subject_uuid2hash=subject_uuidList,
        file_uuid2hash=file_uuidList,
        net_uuid2hash=netflow_uuidList
    )
