##
# Class including all the required methods for handling rds related operations
##

import boto3
import config
import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
from datetime import datetime
import os
import math
import csv

tython_config = config.Config()
conf = tython_config.get_config()
conn = boto3.client('rds',
                    aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                    aws_secret_access_key=conf.get(
                        'aws', 'aws_secret_access_key'),
                    region_name=conf.get('aws', 'region_name'))
suffix = '1479893180'
tables = ["applicant_profile", "applicant_profile_version", "applicant_status", "applicants",
          "applicants_version", "candidate_metadata", "companies", "cv_status", "degrees", "feature_request",
          "interview_details", "interview_details_version", "interview_status", "interview_type",
          "interviewer_interview_relations", "interviewer_status", "load_data", "project_requisition_relations",
          "projects", "requisition", "roles", "rounds", "sources", "test", "transaction", "universities",
          "user_requisition_relations", "users"]


class RdsHandler:
    def get_snapshot_list(object):
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

    def generate_data_csv_file(self):
        cursor = None
        try:
            rds = MySQLdb.connect(
                user=conf.get('hireninja', 'user'),
                passwd=conf.get('hireninja', 'password'),
                host=conf.get('hireninja', 'host'),
                port=int(conf.get('hireninja', 'port')),
                db=conf.get('hireninja', 'db'),
                cursorclass=MySQLdb.cursors.DictCursor)
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

    def upload_csv_files_to_s3(self):
        s3 = boto3.client('s3',
                          aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                          aws_secret_access_key=conf.get('aws', 'aws_secret_access_key'),
                          region_name=conf.get('aws', 'region_name')
                          )
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

    def read_write_binlog_file(self, start_time):
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
