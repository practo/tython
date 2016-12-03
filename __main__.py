##
# Startup file tython
##

from app import helpers, config, Extractor, Loader
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

        db_loader = Loader(db)
        db_loader.generate_redshift_schema()
        db_loader.upload_csv_s3()
        db_loader.upload_csv_redshift()

elif operation == "sync":
    print "sync"
    #TODO write all methods required for sync
else:
    print "Select either copy or sync"