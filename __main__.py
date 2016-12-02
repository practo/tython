##
# Startup file tython
##

import app
from app import helpers, config

conf = config.configuration

options = helpers.app_helper.get_options()

print config.suffix
print options
print conf

operation = options.operation

if operation == "copy":
    print "Creating a copy of database to redshift"
    for db in options.dbs:
        app.create_rds_snapshot_instance(db)
        app.generate_csv_data(db)
        app.generate_redshift_schema(db)
        app.upload_csv_s3(db)
        app.upload_csv_redshift(db)
elif operation == "sync":
    print "sync"
    #TODO write all methods required for sync
else:
    print "Select either copy or sync"