from utils import *
from config import *
import json

def get_node_msg(cur, node_id):
    sql_node = """
            select * from node2id
            where
                  index_id='%s';
            """ % (node_id)
    cur.execute(sql_node)
    msgs = cur.fetchall()
    return msgs[0][1], msgs[0][2]

def select_events(cur):

    node_id = set()

    for day in tqdm(range(8, 10)):
        f = open(artifact_path + "graph_5_" + str(day) + "_new.json", "w")
        start_timestamp = datetime_to_ns_time_US('2019-05-' + str(day) + ' 00:00:00')
        end_timestamp = datetime_to_ns_time_US('2019-05-' + str(day) + ' 00:00:00')
        sql = """
                select * from event_table
                where
                      timestamp_rec>'%s' and timestamp_rec<'%s'
                       ORDER BY timestamp_rec;
                """ % (start_timestamp, end_timestamp)
        cur.execute(sql)
        events = cur.fetchall()
        sort_events = sorted(events, key=lambda x: int(x[5]))

        events_list = list(relMap.keys())
        edges_list = []

        for i in tqdm(range(len(sort_events))):
            e = sort_events[i]
            if e[2] in events_list:
                node_id.add(int(e[1]))
                node_id.add(int(e[4]))

                edges_list.append([e[0], int(e[1]), relMap[e[2]], e[3], int(e[4]), int(e[5]), int(e[6])])

        json.dump(edges_list, f)


if __name__ == "__main__":

    cur, _ = init_database_connection()
    select_events(cur=cur)
