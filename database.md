# Setting Databases

## CADETS E3
```commandline
# execute the psql with postgres user
sudo -u postgres psql

# create the database
postgres=# create database tc_cadet_dataset_db;

# switch to the created database
postgres=# \connect tc_cadet_dataset_db;

# create the event table and grant the privileges to postgres
tc_cadet_dataset_db=# create table event_table
(
    src_node      varchar,
    src_index_id  varchar,
    operation     varchar,
    dst_node      varchar,
    dst_index_id  varchar,
    timestamp_rec bigint,
    _id           serial
);
tc_cadet_dataset_db=# alter table event_table owner to postgres;
tc_cadet_dataset_db=# create unique index event_table__id_uindex on event_table (_id); grant delete, insert, references, select, trigger, truncate, update on event_table to postgres;

# create the file table
tc_cadet_dataset_db=# create table file_node_table
(
    node_uuid varchar not null,
    path      varchar,
    constraint file_node_table_pk
        primary key (node_uuid)
);
tc_cadet_dataset_db=# alter table file_node_table owner to postgres;

# create the netflow table
tc_cadet_dataset_db=# create table netflow_node_table
(
    node_uuid varchar not null,
    src_addr  varchar,
    src_port  varchar,
    dst_addr  varchar,
    dst_port  varchar,
    constraint netflow_node_table_pk
        primary key (node_uuid)
);
tc_cadet_dataset_db=# alter table netflow_node_table owner to postgres;

# create the subject table
tc_cadet_dataset_db=# create table subject_node_table
(
    node_uuid varchar,
    exec      varchar,
    constraint subject_node_table_pk
        primary key (node_uuid)
);
tc_cadet_dataset_db=# alter table subject_node_table owner to postgres;

# create the node2id table
tc_cadet_dataset_db=# create table node2id
(
    node_uuid   varchar,
    node_type varchar,
    msg       varchar,
    index_id  bigint
);
tc_cadet_dataset_db=# alter table node2id owner to postgres;
```


## THEIA E3
```commandline
# execute the psql with postgres user
sudo -u postgres psql

# create the database
postgres=# create database tc_theia_dataset_db;

# switch to the created database
postgres=# \connect tc_theia_dataset_db;

# create the event table and grant the privileges to postgres
tc_theia_dataset_db=# create table event_table
(
    src_node      varchar,
    src_index_id  varchar,
    operation     varchar,
    dst_node      varchar,
    dst_index_id  varchar,
    timestamp_rec bigint,
    _id           serial
);
tc_theia_dataset_db=# alter table event_table owner to postgres;
tc_theia_dataset_db=# create unique index event_table__id_uindex on event_table (_id); grant delete, insert, references, select, trigger, truncate, update on event_table to postgres;

# create the file table
tc_theia_dataset_db=# create table file_node_table
(
    node_uuid varchar not null,
    path      varchar,
    constraint file_node_table_pk
        primary key (node_uuid)
);
tc_theia_dataset_db=# alter table file_node_table owner to postgres;

# create the netflow table
tc_theia_dataset_db=# create table netflow_node_table
(
    node_uuid varchar not null,
    src_addr  varchar,
    src_port  varchar,
    dst_addr  varchar,
    dst_port  varchar,
    constraint netflow_node_table_pk
        primary key (node_uuid)
);
tc_theia_dataset_db=# alter table netflow_node_table owner to postgres;

# create the subject table
tc_theia_dataset_db=# create table subject_node_table
(
    node_uuid varchar not null,
    "cmdLine" varchar not null,
    tgid      varchar not null,
    path      varchar not null,
    constraint subject_node_table_pk
        primary key (node_uuid)
);
tc_theia_dataset_db=# alter table subject_node_table owner to postgres;

# create the node2id table
tc_theia_dataset_db=# create table node2id
(
    node_uuid   varchar,
    node_type varchar,
    msg       varchar,
    index_id  bigint
);
tc_theia_dataset_db=# alter table node2id owner to postgres;
```


## CADETS E5
```commandline
# execute the psql with postgres user
sudo -u postgres psql

# create the database
postgres=# create database tc_e5_cadets_dataset_db;

# switch to the created database
postgres=# \connect tc_e5_cadets_dataset_db;

# create the event table and grant the privileges to postgres
tc_e5_cadets_dataset_db=# create table event_table
(
    src_node      varchar,
    src_index_id  varchar,
    operation     varchar,
    dst_node      varchar,
    dst_index_id  varchar,
    timestamp_rec bigint,
    _id           serial
);
tc_e5_cadets_dataset_db=# alter table event_table owner to postgres;
tc_e5_cadets_dataset_db=# create unique index event_table__id_uindex on event_table (_id); grant delete, insert, references, select, trigger, truncate, update on event_table to postgres;

# create the file table
tc_e5_cadets_dataset_db=# create table file_node_table
(
    node_uuid varchar not null,
    path      varchar,
    constraint file_node_table_pk
        primary key (node_uuid)
);
tc_e5_cadets_dataset_db=# alter table file_node_table owner to postgres;

# create the netflow table
tc_e5_cadets_dataset_db=# create table netflow_node_table
(
    node_uuid varchar not null,
    src_addr  varchar,
    src_port  varchar,
    dst_addr  varchar,
    dst_port  varchar,
    constraint netflow_node_table_pk
        primary key (node_uuid)
);
tc_e5_cadets_dataset_db=# alter table netflow_node_table owner to postgres;

# create the subject table
tc_e5_cadets_dataset_db=# create table subject_node_table
(
    node_uuid varchar,
    exec      varchar,
    constraint subject_node_table_pk
        primary key (node_uuid)
);
tc_e5_cadets_dataset_db=# alter table subject_node_table owner to postgres;

# create the node2id table
tc_e5_cadets_dataset_db=# create table node2id
(
    node_type varchar,
    msg       varchar,
    index_id  bigint
);
tc_e5_cadets_dataset_db=# alter table node2id owner to postgres;
```


## THEIA E5
```commandline
# execute the psql with postgres user
sudo -u postgres psql

# create the database
postgres=# create database tc_e5_theia_dataset_db;

# switch to the created database
postgres=# \connect tc_e5_theia_dataset_db;

# create the event table and grant the privileges to postgres
tc_e5_theia_dataset_db=# create table event_table
(
    src_node      varchar,
    src_index_id  varchar,
    operation     varchar,
    dst_node      varchar,
    dst_index_id  varchar,
    timestamp_rec bigint,
    _id           serial
);
tc_e5_theia_dataset_db=# alter table event_table owner to postgres;
tc_e5_theia_dataset_db=# create unique index event_table__id_uindex on event_table (_id); grant delete, insert, references, select, trigger, truncate, update on event_table to postgres;

# create the file table
tc_e5_theia_dataset_db=# create table file_node_table
(
    node_uuid varchar not null,
    path      varchar,
    constraint file_node_table_pk
        primary key (node_uuid)
);
tc_e5_theia_dataset_db=# alter table file_node_table owner to postgres;

# create the netflow table
tc_e5_theia_dataset_db=# create table netflow_node_table
(
    node_uuid varchar not null,
    src_addr  varchar,
    src_port  varchar,
    dst_addr  varchar,
    dst_port  varchar,
    constraint netflow_node_table_pk
        primary key (node_uuid)
);
tc_e5_theia_dataset_db=# alter table netflow_node_table owner to postgres;

# create the subject table
tc_e5_theia_dataset_db=# create table subject_node_table
(
    node_uuid varchar not null,
    exec      varchar
);
tc_e5_theia_dataset_db=# alter table subject_node_table owner to postgres;

# create the node2id table
tc_e5_theia_dataset_db=# create table node2id
(
    node_uuid   varchar not null,
    node_type varchar,
    msg       varchar,
    index_id  bigint
);
tc_e5_theia_dataset_db=# alter table node2id owner to postgres;
```


