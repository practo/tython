##
# Startup file tython
##

from app import helpers, config, \
    Extractor, Loader, Sync
import sys

conf = config.configuration

options = helpers.app_helper.get_options()

operation = options.operation

if len(options.dbs) is 0 :
    print "No database to copy"
    sys.exit(1)

if operation == "copy":
    print "Creating a copy of database to redshift"
    for db in options.dbs:
        db_extractor = Extractor(db)
        db_extractor.create_rds_snapshot_instance()
        db_extractor.generate_csv_data()
        db_extractor.destroy_rds()

        db_loader = Loader(db)
        db_loader.generate_redshift_schema()
        db_loader.upload_csv_s3()
        db_loader.upload_csv_redshift()

elif operation == "sync":
    print "Starting sync"
    for db in options.dbs:
        db_sync = Sync(db)
        db_sync.read_mysql_bin_log()
else:
    print "Select either copy or sync"