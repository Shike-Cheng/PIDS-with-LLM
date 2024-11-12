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
    for day in tqdm(range(2, 7)):
        f = open(artifact_path + "graph_4_" + str(day) + ".json", "w")
        start_timestamp = datetime_to_ns_time_US('2018-04-' + str(day) + ' 00:00:00')
        print(start_timestamp)
        end_timestamp = datetime_to_ns_time_US('2018-04-' + str(day + 1) + ' 00:00:00')
        print(end_timestamp)
        sql = """
                select * from event_table
                where
                      timestamp_rec>='%s' and timestamp_rec<='%s'
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
                edges_list.append([e[0], int(e[1]), relMap[e[2]], e[3], int(e[4]), int(e[5]), int(e[6])])

        json.dump(edges_list, f)



if __name__ == "__main__":

    cur, _ = init_database_connection()
    select_events(cur=cur)

