##
# Class containgin all the required methods for data differential extractions
##

import rds_handler


def create_rds_snapshot_instance():
    print("Creating rds instance using snapshot")
    rds_handler_object = rds_handler.RdsHandler()
    latest_snapshot = rds_handler_object.get_snapshot_list()
    # rds_handler_object.create_rds_instance_from_snapshot(latest_snapshot)
