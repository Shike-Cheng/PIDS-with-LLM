
# sudo systemctl start postgresql
# sudo -u postgres psql

########################################################
#
#               Database settings
#
########################################################

# Database name
database = 'tc_e5_theia_dataset_db'

# Only config this setting when you have the problem mentioned
# in the Troubleshooting section in settings/environment-settings.md.
# Otherwise, set it as None
host = '/var/run/postgresql/'
# host = None

# Database user
user = 'postgres'

# The password to the database user
password = 'postgres'

# The port number for Postgres
port = '5432'

########################################################
#
#                   Artifacts path
#
########################################################

# The directory of the raw logs
# raw_dir = ""
artifact_path = "./artifact/"
########################################################
#
#               Graph semantics
#
########################################################

# The directions of the following edge types need to be reversed
edge_reversed = [
    "EVENT_READ",
    "EVENT_MMAP",
    "EVENT_RECVFROM",
    "EVENT_RECVMSG"
]

# The following edges are the types only considered to construct the
# temporal graph for experiments.
# include_edge_type=[
#     "EVENT_WRITE",
#     "EVENT_READ",
#     "EVENT_MMAP",
#     "EVENT_SENDTO",
#     "EVENT_RECVFROM",
#     "EVENT_EXECUTE",
#     "EVENT_FORK"
# ]

# The map between edge type and edge ID
# rel2id = {
#  1: 'EVENT_WRITE',
#  'EVENT_WRITE': 1,
#  2: 'EVENT_READ',
#  'EVENT_READ': 2,
#  3: 'EVENT_MMAP',
#  'EVENT_MMAP': 3,
#  4: 'EVENT_SENDTO',
#  'EVENT_SENDTO': 4,
#  5: 'EVENT_RECVFROM',
#  'EVENT_RECVFROM': 5,
#  6: 'EVENT_OPEN',
#  'EVENT_OPEN': 6,
#  7: 'EVENT_EXECUTE',
#  'EVENT_EXECUTE': 7,
#  8: 'EVENT_FORK',
# 'EVENT_FORK': 8
# }

relMap = {
    "EVENT_WRITE" : "EVENT_WRITE",
    "EVENT_READ" : "EVENT_READ",
    "EVENT_MMAP" : "EVENT_MMAP",
    "EVENT_SENDTO" : "EVENT_SENDTO",
    "EVENT_RECVFROM" : "EVENT_RECVFROM",
    "EVENT_EXECUTE" : "EVENT_EXECUTE",
    "EVENT_FORK" : "EVENT_FORK",
    "EVENT_SENDMSG" : "EVENT_SENDTO",
    "EVENT_RECVFMSG" : "EVENT_RECVFROM"
}
