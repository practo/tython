##
# Class including all the required methods for handling rds related operations
##

import os
import sys
from datetime import datetime

import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import boto3

from app.config import config

conf = config.get_config()
conn = boto3.client('rds',
                    aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                    aws_secret_access_key=conf.get(
                        'aws', 'aws_secret_access_key'),
                    region_name=conf.get('aws', 'region_name'))
suffix = '1479893180'

def get_db_tables():
    try:
        rds = MySQLdb.connect(
            user=conf.get('hireninja', 'user'),
            passwd=conf.get('hireninja', 'password'),
            host=conf.get('hireninja', 'host'),
            port=int(conf.get('hireninja', 'port')),
            db=conf.get('hireninja', 'db'),
            cursorclass=MySQLdb.cursors.DictCursor)
        rds_cursor = rds.cursor()
        rds_cursor.execute(
            "select group_concat(TABLE_NAME) as tables from information_schema.tables where TABLE_SCHEMA = \"hireninja\"")
        tables = rds_cursor.fetchall()[0]['tables'].split(',')
        rds_cursor.close()
        return tables
    except _mysql_exceptions.Error, err:
        print(err)
        sys.exit(1)

def get_snapshot_list():
    snapshots = conn.describe_db_snapshots(
        DBInstanceIdentifier=conf.get('aws', 'databases'),
        MaxRecords=100
    )['DBSnapshots']
    snapshots = filter(lambda x: x.get('SnapshotCreateTime'), snapshots)
    latest_snapshot = max(snapshots, key=lambda s: s['SnapshotCreateTime'])
    return latest_snapshot


def create_rds_instance_from_snapshot(object, latest_snapshot):
    db_instance_identifier = 'tython-' + conf.get('aws', 'databases') + \
                             "-" + suffix
    response = conn.restore_db_instance_from_db_snapshot(
        DBSnapshotIdentifier=latest_snapshot['DBSnapshotIdentifier'],
        DBInstanceIdentifier=db_instance_identifier,
        PubliclyAccessible=True,
        MultiAZ=False
    )
    print response


def generate_data_csv_file():
    cursor = None
    try:
        rds = MySQLdb.connect(
            user=conf.get('hireninja', 'user'),
            passwd=conf.get('hireninja', 'password'),
            host=conf.get('hireninja', 'host'),
            port=int(conf.get('hireninja', 'port')),
            db=conf.get('hireninja', 'db'),
            cursorclass=MySQLdb.cursors.DictCursor)
        tables = get_db_tables()
        tables.pop(0)
        for table in tables:
            rds_cursor = rds.cursor()
            rds_cursor.execute(
                "select group_concat(COLUMN_NAME) as columns from information_schema.COLUMNS where TABLE_SCHEMA = \"hireninja\" and TABLE_NAME = \"" + table + "\"");
            column = rds_cursor.fetchall()
            column = column[0]["columns"]
            column = column.split(',')
            column_val = []
            for col in column:
                column_val.append("`" + col + "`")
            print "select " + ",".join(column_val) + " from " + table
            rds_cursor.execute("select " + ",".join(column_val) + " from " + table);
            result = rds_cursor.fetchall()
            fp = open("/Users/sandeep/Documents/project/code/experiments/tython/csv_files/" + table + ".csv", "wb")
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
    except _mysql_exceptions.Error, err:
        print(err)
        sys.exit(1)


def upload_csv_files_to_s3():
    s3 = boto3.client('s3',
                      aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                      aws_secret_access_key=conf.get('aws', 'aws_secret_access_key'),
                      region_name=conf.get('aws', 'region_name')
                      )
    tables = get_db_tables()
    for table in tables:
        tempfile = "/Users/sandeep/Documents/project/code/experiments/tython/csv_files/" + table + ".csv"
        fil = open(tempfile, "rb")
        s3_path = "tipocaData/bckp_" + suffix + "/" + table
        response = s3.put_object(
            Bucket=conf.get('aws', 'bucket'),
            Key=s3_path,
            ACL="private",
            Body=fil
        )


def read_write_binlog_file(start_time):
    cursor = None
    mysql_bin_log_command = "mysqlbinlog -v \
                --read-from-remote-server \
                --host=test\
                --port=3306  \
                --user test \
                --password=test#\
                --stop-never \
                --start-datetime=\"" + str(start_time) + "\" \
                --result-file=/Users/sandeep/Documents/project/code/experiments/tython/tmp/bin.log \
                mysql-bin-changelog.001879"
    os.system(mysql_bin_log_command)
