##
# Class containgin all the required methods for data differential extractions
##

import time
from datetime import datetime
from helpers import rds_handler, redshift_handler, app_helper

from app.helpers import rds_handler


def create_rds_snapshot_instance(db_name):
    print("Creating rds instance using snapshot")
    latest_snapshot = rds_handler.get_snapshot_list(db_name)
    rds_handler.create_rds_instance_from_snapshot(db_name, latest_snapshot)

def generate_csv_data(db_name):
    rds_handler.generate_data_csv_file(db_name)

def generate_redshift_schema(db_name):
    rds_handler.create_redshift_schema(db_name)

def upload_csv_s3(db_name):
    app_helper.upload_csv_files_to_s3(db_name)

def upload_csv_redshift(db_name):
    redshift_handler.upload_csv_redshift(db_name)

# def read_binglog_file(start_time):
#     rds_handler.read_write_binlog_file(start_time)
#     #redshift_handler_object.update_data_using_binlog()
