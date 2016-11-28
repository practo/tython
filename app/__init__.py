##
# Class containgin all the required methods for data differential extractions
##

import rds_handler, redshift_handler
import time
from datetime import datetime, date
import dateutil.parser

def create_rds_snapshot_instance():
    print("Creating rds instance using snapshot")
    rds_handler_object = rds_handler.RdsHandler()
    redshift_handler_object = redshift_handler.RedshiftHandler()
    latest_snapshot = rds_handler_object.get_snapshot_list()
    print latest_snapshot
    print latest_snapshot["SnapshotCreateTime"]
    print time.mktime(latest_snapshot["SnapshotCreateTime"].timetuple())
    print datetime.utcfromtimestamp(time.mktime(latest_snapshot["SnapshotCreateTime"].timetuple()))
    #time.mktime(latest_snapshot["SnapshotCrateTime"])
    #rds_handler_object.create_rds_instance_from_snapshot(latest_snapshot)
    #rds_handler_object.create_redshift_schema()
    #rds_handler_object.generate_data_csv_file()
    #rds_handler_object.upload_csv_files_to_s3()
    #redshift_handler_object.upload_csv_redshift()

