##
# Class containgin all the required methods for data differential extractions
##

from helpers import rds_handler, redshift_handler, app_helper


class Extractor:
    def __init__(self, db_name):
        self.db_name = db_name
        self.rds = rds_handler.rds(db_name)

    def create_rds_snapshot_instance(self):
        print("Creating rds instance using snapshot")
        latest_snapshot = self.rds.get_snapshot_list()
        self.rds.create_rds_instance_from_snapshot(latest_snapshot)
        self.rds.verify_rds_creation()

    def generate_csv_data(self):
        self.rds.generate_data_csv_file()

    def destroy_rds(self):
        self.rds.destroy_rds_instances()

class Loader:
    def __init__(self, db_name):
        self.db_name = db_name
        self.rds = rds_handler.rds(db_name)

    def generate_redshift_schema(self):
        redshift_handler.create_redshift_schema(self.db_name)

    def upload_csv_s3(self):
        app_helper.upload_csv_files_to_s3(self.db_name)

    def upload_csv_redshift(self):
        redshift_handler.upload_csv_redshift(self.db_name)

# def read_binglog_file(start_time):
#     rds_handler.read_write_binlog_file(start_time)
#     #redshift_handler_object.update_data_using_binlog()
