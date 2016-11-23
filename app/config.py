##
# Config parser for tython
##

import ConfigParser
import os


class Config:

    def get_config(object):
        config = ConfigParser.ConfigParser()
        config.read(os.getcwd() + '/app/config.ini')
        return config
