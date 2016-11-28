##
# Class including all the required methods for handling rds related operations
##

import boto3
import config
import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
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
            rds_cursor = rds.cursor()
            for table in tables:
                rds_cursor.execute("select * from " + table);
                result = rds_cursor.fetchall()
                myfile = open("/Users/sandeep/Documents/project/code/experiments/tython/csv_files/" + table + ".csv", 'wb')
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
                wr.writerow(result)
        except _mysql_exceptions.Error, err:
            print(err)
            sys.exit(1)

    def upload_csv_files_to_s3(self):
        s3 = boto3.client('s3',
                          aws_access_key_id=conf.get('aws','aws_access_key_id'),
                          aws_secret_access_key=conf.get('aws','aws_secret_access_key'),
                          region_name=conf.get('aws', 'region_name')
                          )
        for table in tables:
            tempfile = "/Users/sandeep/Documents/project/code/experiments/tython/csv_files/"+ table + ".csv"
            fil = open(tempfile, "rb")
            s3_path = "tipocaData/bckp_" + suffix + "/" + table
            response = s3.put_object(
                Bucket=conf.get('aws', 'bucket'),
                Key=s3_path,
                ACL="private",
                Body=fil
            )