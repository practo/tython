
import os
import yaml
import time

def read_config_yml():
    file = open(os.getcwd() + '/app/config/config.yml' , "rb")
    config = yaml.load(file)
    return config

configuration = read_config_yml()

suffix = str(time.time()).split(".")[0]