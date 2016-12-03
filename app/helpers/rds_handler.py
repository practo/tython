##
# Module including all the required methods for handling rds related operations
##

import os
import sys
from datetime import datetime
import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import boto3
import botocore
import copy
import time

from app import config

max_retries = 460

conf = config.configuration
conn = boto3.client('rds',
                    aws_access_key_id=conf['aws']['aws_access_key_id'],
                    aws_secret_access_key=conf['aws']['aws_secret_access_key'],
                    region_name=conf['aws']['region_name'])
suffix = config.suffix


class rds:
    def __init__(self, db_name):
        self.scan_interval = 15
        self.db_name = db_name
        self.db_instance_identifier = 'tython-' + db_name + \
                                      "-" + suffix
        self.new_rds_credentials = copy.deepcopy(conf['rds'][self.db_name])

    def get_rds_db_conn(self):
        try:
            db_conf = self.new_rds_credentials
            rds = MySQLdb.connect(
                user=db_conf['user'],
                passwd=db_conf['password'],
                host=db_conf['host'],
                port=db_conf['port'],
                db=db_conf['db'],
                cursorclass=MySQLdb.cursors.DictCursor)
            return rds
        except _mysql_exceptions.Error, err:
            print(err)
            sys.exit(1)

    def get_db_tables(self):
        try:
            rds = self.get_rds_db_conn()
            rds_cursor = rds.cursor()
            rds_cursor.execute(
                "select group_concat(TABLE_NAME) as tables from information_schema.tables \
                where TABLE_SCHEMA = \"" + self.db_name + "\"")
            tables = rds_cursor.fetchall()[0]['tables'].split(',')
            rds_cursor.close()
            rds.close()
            return tables
        except _mysql_exceptions.Error, err:
            print(err)
            rds_cursor.close()
            rds.close()
            sys.exit(1)

    def get_snapshot_list(self):
        snapshots = conn.describe_db_snapshots(
            DBInstanceIdentifier=self.db_name,
            MaxRecords=100
        )['DBSnapshots']
        snapshots = filter(lambda x: x.get('SnapshotCreateTime'), snapshots)
        latest_snapshot = max(snapshots, key=lambda s: s['SnapshotCreateTime'])
        return latest_snapshot

    def create_rds_instance_from_snapshot(self, latest_snapshot):
        response = conn.restore_db_instance_from_db_snapshot(
            DBSnapshotIdentifier=latest_snapshot['DBSnapshotIdentifier'],
            DBInstanceIdentifier=self.db_instance_identifier,
            PubliclyAccessible=True,
            MultiAZ=False
        )
        print response

    def verify_rds_creation(self):
        max_retries = 460
        while max_retries > 0:
            try:
                response = conn.describe_db_instances(
                    DBInstanceIdentifier=self.db_instance_identifier,
                    MaxRecords=20
                )
            except botocore.exceptions.clientErro, err:
                print(err)
                sys.exit(1)
            dbi = None
            for db_instance in response['DBInstances']:
                if db_instance['DBInstanceIdentifier'] == self.db_instance_identifier:
                    dbi = db_instance
                    break
            if dbi != None and dbi['DBInstanceStatus'] == "available" and not dbi['PendingModifiedValues']:
                checks -= 1
                if checks == 0:
                    new_rds_credentials = copy.deepcopy(conf['rds'][self.db_name])
                    new_rds_credentials['host'] = dbi['Endpoint']['Address']
                    new_rds_credentials['port'] = dbi['Endpoint']['Port']
                    self.new_rds_credentials = new_rds_credentials
                    break
                else:
                    time.sleep(self.scan_interval)
                    continue
            checks = 2
            if max_retries == 0:
                print("RDS Instance creation timed out for %s" % self.db_name)
                sys.exit(1)
            max_retries -= 1
            time.sleep(self.scan_interval)

    def generate_data_csv_file(self):
        try:
            tables = self.get_db_tables()
            rds = self.get_rds_db_conn()
            for table in tables:
                rds_cursor = rds.cursor()
                rds_cursor.execute(
                    "select group_concat(COLUMN_NAME) as columns from information_schema.COLUMNS \
                    where TABLE_SCHEMA = \"" + self.db_name + "\" and TABLE_NAME = \"" + table + "\"");
                column = rds_cursor.fetchall()
                column = column[0]["columns"]
                column = column.split(',')
                column_val = []
                for col in column:
                    column_val.append("`" + col + "`")
                print "select " + ",".join(column_val) + " from " + table
                rds_cursor.execute("select " + ",".join(column_val) + " from " + table);
                result = rds_cursor.fetchall()
                fp = open(conf['csv']['path'] + table + ".csv", "wb")
                for row in result:
                    col_data = []
                    for col in column:
                        col = row[col]
                        if col is None:
                            col_data.append(u"")
                        elif isinstance(col, datetime):
                            temp = unicode(col.isoformat(" ").split(".")[0])
                            col_data.append(u'"' + temp + u'"')
                        elif isinstance(col, str):
                            temp = col
                            temp = temp.replace("\0", "")
                            if len(temp) > 20000:
                                temp = temp[0:20000]
                            temp = temp.replace('"', '""')
                            temp = temp.decode('unicode_escape')
                            col_data.append(u'"' + unicode(temp) + u'"')
                        elif isinstance(col, unicode):
                            temp = col
                            temp = temp.replace("\0", "")
                            if len(temp) > 20000:
                                temp = temp[0:20000]
                            temp = temp.replace('"', '""')
                            col_data.append(u'"' + temp + u'"')
                        else:
                            col_data.append(unicode(str(col)))
                    row_csv = u",".join(col_data) + u"\n"
                    fp.write(row_csv.encode('utf-8'))
                fp.close()
                rds_cursor.close()
            rds.close()
        except _mysql_exceptions.Error, err:
            print(err)
            rds_cursor.close()
            rds.close()
            sys.exit(1)

    def destroy_rds_instances(self):
        try:
            conn.delete_db_instance(
                DBInstanceIdentifier=self.db_instance_identifier,
                SkipFinalSnapshot=True
            )
        except botocore.exceptions.ClientError as err:
            print err
            sys(1)

    def get_db_master_status(self):
        try:
            rds = self.get_rds_db_conn()
            rds_cursor = rds.cursor()
            rds_cursor.execute("SHOW MASTER STATUS")
            master_status = rds_cursor.fetchall()[0]
            rds_cursor.close()
            rds.close()
            return master_status
        except _mysql_exceptions, err:
            print(err)
            rds_cursor.close()
            rds.close()
            sys.exit(1)

    def read_write_binlog_file(self, master_status):
        mysql_bin_log_command = "mysqlbinlog -v \
                    --read-from-remote-server \
                    --host=" + conf['rds'][self.db_name]['host'] + "\
                    --port=" + str(conf['rds'][self.db_name]['port']) + " \
                    --user=" + conf['rds'][self.db_name]['user'] + "\
                    --password=" + conf['rds'][self.db_name]['password'] + "\
                    --stop-never --start-position=" + str(master_status['Position']) + "\
                    --result-file=" + conf['binlog']['path'] + " " +\
                                str(master_status['File'])
        print mysql_bin_log_command
        os.system(mysql_bin_log_command)
