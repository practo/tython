##
# Config parser for tython
##

import ConfigParser
import os


def get_config():
    config = ConfigParser.ConfigParser()
    config.read(os.getcwd() + '/app/config/config.ini')
    return config
