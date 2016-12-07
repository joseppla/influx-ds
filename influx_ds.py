#!/usr/bin/python
import sys
import argparse
from influxdb import InfluxDBClient


# Setting up parser arguments
PARSER = argparse.ArgumentParser()
PARSER.add_argument('-s', '--server', action='store', dest='SERVER',
                    help='Ip address of the InfluxDB server, default 127.0.0.1', default='127.0.0.1')
PARSER.add_argument('-p', '--port', action='store', dest='PORT',
                    help='InfluxDB port, default 8086', default=8086)
PARSER.add_argument('-d', '--database', action='store', dest='DATABASE',
                    help='Database name, default telegraf', default='telegraf')
PARSER.add_argument('-n', '--name', action='store', dest='POLICY_NAME',
                    help='New or existent policy name, default telegraf_ds', default='telegraf_ds')
PARSER.add_argument('-l', '--duration', action='store', dest='POLICY_DURATION',
                    help='Duration of the data inside the created policy, default 104w', default='104w')
PARSER.add_argument('-a', '--ago', action='store', dest='AGO',
                    help='Selects how many days to keep without downsampling data, default 7d', default='7d')
PARSER.add_argument('-t', '--autogen-duration', action='store', dest='AUTOGEN_DURATION',
                    help='Sets the autogen new duration, default 168h', default='168h')
PARSER.add_argument('-g', '--group-by', action='store', dest='GROUP_BY',
                    help='New downsampled time resolution, default 30m', default='30m')
PARSER.add_argument('-u', '--update_policy', action='store', dest='UPDATE',
                    help="Update the policy if exists, default False", default=False)
PARSER.add_argument('-o', '--overwrite', action='store', dest='OVERWRITE',
                    help="Overwrite the query if exists, default False", default=False)

# Checkinf if user passed any parameter
if not len(sys.argv) > 1:
    PARSER.print_help()
    sys.exit()

# Getting params
PARAMS = PARSER.parse_args()

AUTOGEN_NAME = 'autogen'
SERVER = PARAMS.SERVER
PORT = PARAMS.PORT
DATABASE = PARAMS.DATABASE
POLICY_NAME = PARAMS.POLICY_NAME
POLICY_DURATION = PARAMS.POLICY_DURATION
AGO = PARAMS.AGO
AUTOGEN_DURATION = PARAMS.AUTOGEN_DURATION
GROUP_BY = PARAMS.GROUP_BY
UPDATE = PARAMS.UPDATE
OVERWRITE = PARAMS.OVERWRITE


# Connecting to database and switching to desired database
DB_CONNECTION = InfluxDBClient(SERVER, PORT)
DB_CONNECTION.switch_database(DATABASE)

# Generating the downsampled retention Policy if not exists, if exists
# altering it
if POLICY_NAME in [d['name'] for d in DB_CONNECTION.get_list_retention_policies(database=DATABASE)] and UPDATE:
    print "Policy {pn} already exists, updating...".format(pn=POLICY_NAME)
    DB_CONNECTION.alter_retention_policy(
        POLICY_NAME, duration=POLICY_DURATION, replication=1)
elif POLICY_NAME in [d['name'] for d in DB_CONNECTION.get_list_retention_policies(database=DATABASE)] and not UPDATE:
    print "The policy {pn} already exists and the update flag is set to False".format(pn=POLICY_NAME)
else:
    print "The policy {pn} doesn't exists, creating...".format(pn=POLICY_NAME)
    DB_CONNECTION.create_retention_policy(
        POLICY_NAME, duration=POLICY_DURATION, replication=1)

# Getting all existent measurements and continuous queries in database
MEASUREMENTS = [d['name'] for d in list(
    DB_CONNECTION.query('show measurements;').get_points())]
CQ_LIST = [d['name']
           for d in list(DB_CONNECTION.query('show continuous queries;'))[1]]

''' Looping through all measurements and appying a continuous query
on each key for each measurement for downsampling purposes '''

for m in MEASUREMENTS:
    KEYS = [d['fieldKey'] for d in list(DB_CONNECTION.query(
        'show field keys from {keys}'.format(keys=m)).get_points())]
    for k in KEYS:
        Q = 'CREATE CONTINUOUS QUERY "{m_name}_{k_name}_ds" on "{db_name}" BEGIN SELECT mean("{k_name}") as "{k_name}" into "{db_name}"."{p_name}"."{m_name}" from "{m_name}" where time > now() - "{AGO}" group by time({d_time}) END'.format(
            k_name=k, db_name=DATABASE, p_name=POLICY_NAME, m_name=m, AGO=AGO, d_time=GROUP_BY)
        # Checking if the query name exists, if exists we delete it and create
        # a new one, if not we create it
        if '{m_name}_{k_name}_ds'.format(k_name=k, m_name=m) in CQ_LIST:
            if OVERWRITE:
                print 'Query {m_name}_{k_name}_ds already exists on {db_name}, deleting existent one and creating a new one'.format(k_name=k, db_name=DATABASE, m_name=m)
                result = DB_CONNECTION.query('DROP CONTINUOUS QUERY "{m_name}_{k_name}_ds" ON "{db_name}"'.format(
                    m_name=m, k_name=k, db_name=DATABASE))
                result = DB_CONNECTION.query(Q)
            else:
                print 'Query {m_name}_{k_name}_ds already exists on {db_name} and the overwrite flag is set to False'.format(k_name=k, m_name=m, db_name=DATABASE)
        else:
            print "Creating CQ {m_name}_{k_name}_ds on {db_name}".format(k_name=k, db_name=DATABASE, m_name=m)
            result = DB_CONNECTION.query(Q)
