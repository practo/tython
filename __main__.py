##
# Startup file tython
##

import app
import time

from app import helpers

config = app.config.configuration

print helpers
options = helpers.app_helper.get_options()

suffix = time.time()
print options
print config
#app.create_rds_snapshot_instance()