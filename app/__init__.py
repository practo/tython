##
# Class containgin all the required methods for data differential extractions
##

import time
from datetime import datetime
from helpers import rds_handler, redshift_handler

from app.helpers import rds_handler


def create_rds_snapshot_instance():
    print("Creating rds instance using snapshot")
    latest_snapshot = rds_handler.get_snapshot_list()
    print latest_snapshot
    print latest_snapshot["SnapshotCreateTime"]
    print time.mktime(latest_snapshot["SnapshotCreateTime"].timetuple())
    timeStamp = datetime.fromtimestamp(time.mktime(latest_snapshot["SnapshotCreateTime"].timetuple()))
    #time.mktime(latest_snapshot["SnapshotCrateTime"])
    #rds_handler_object.create_rds_instance_from_snapshot(latest_snapshot)
    #rds_handler_object.create_redshift_schema()
    #rds_handler_object.generate_data_csv_file()
    #rds_handler_object.upload_csv_files_to_s3()
    #redshift_handler_object.upload_csv_redshift()
    #rds_handler_object.read_write_binlog_file(timeStamp)
    #redshift_handler_object.update_data_using_binlog()
