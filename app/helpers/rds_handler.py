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

from app import config

conf = config.configuration
conn = boto3.client('rds',
                    aws_access_key_id=conf['aws']['aws_access_key_id'],
                    aws_secret_access_key=conf['aws']['aws_secret_access_key'],
                    region_name=conf['aws']['region_name'])
suffix = config.suffix


def get_rds_db_conn(db_name):
    try:
        db_conf = conf['rds'][db_name]
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


def get_db_tables(db_name):
    try:
        rds = get_rds_db_conn(db_name)
        rds_cursor = rds.cursor()
        rds_cursor.execute(
            "select group_concat(TABLE_NAME) as tables from information_schema.tables \
            where TABLE_SCHEMA = \"" + db_name + "\"")
        tables = rds_cursor.fetchall()[0]['tables'].split(',')
        rds_cursor.close()
        rds.close()
        return tables
    except _mysql_exceptions.Error, err:
        print(err)
        rds_cursor.close()
        rds.close()
        sys.exit(1)


def get_snapshot_list(db_name):
    snapshots = conn.describe_db_snapshots(
        DBInstanceIdentifier=db_name,
        MaxRecords=100
    )['DBSnapshots']
    snapshots = filter(lambda x: x.get('SnapshotCreateTime'), snapshots)
    latest_snapshot = max(snapshots, key=lambda s: s['SnapshotCreateTime'])
    return latest_snapshot


def create_rds_instance_from_snapshot(db_name, latest_snapshot):
    db_instance_identifier = 'tython-' + db_name + \
                             "-" + suffix
    response = conn.restore_db_instance_from_db_snapshot(
        DBSnapshotIdentifier=latest_snapshot['DBSnapshotIdentifier'],
        DBInstanceIdentifier=db_instance_identifier,
        PubliclyAccessible=True,
        MultiAZ=False
    )
    print response


def generate_data_csv_file(db_name):
    try:
        tables = get_db_tables(db_name)
        tables.pop(0)
        rds = get_rds_db_conn(db_name)
        for table in tables:
            rds_cursor = rds.cursor()
            rds_cursor.execute(
                "select group_concat(COLUMN_NAME) as columns from information_schema.COLUMNS \
                where TABLE_SCHEMA = \"" + db_name + "\" and TABLE_NAME = \"" + table + "\"");
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


def read_write_binlog_file(start_time, db_name):
    try:
        rds = get_rds_db_conn(db_name)
        rds_cursor = rds.cursor()
        bin_log_files = rds_cursor.execute("SHOW BINARY LOGS")
        bin_log_files = bin_log_files.fetchall()[0]
        start_file = bin_log_files["Log_name"]
        rds_cursor.close()
        rds.close()
    except _mysql_exceptions, err:
        print(err)
        rds_cursor.close()
        rds.close()
        sys(1)

    mysql_bin_log_command = "mysqlbinlog -v \
                --read-from-remote-server \
                --host=" + config['rds'][db_name]['host'] \
                            + "--port=" + config['rds'][db_name]['port'] \
                            + "--user=" + config['rds'][db_name]['user'] \
                            + "--password=" + config['rds'][db_name]['password'] \
                            + "--stop-never \
                --start-datetime=\"" + str(start_time) + "\" \
                --result-file=" + config['binlog']['path'] \
                            + start_file
    os.system(mysql_bin_log_command)
