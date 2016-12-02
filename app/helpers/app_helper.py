import optparse
import boto3
import botocore
import rds_handler

from app import config

conf = config.configuration
suffix = config.suffix

def __get_comma_separated_args(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))

def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-d', '--databases',
                      type='string',
                      action='callback',
                      callback=__get_comma_separated_args,
                      dest="dbs",
                      default=[]
                      )
    parser.add_option('-m', '--no-monitoring',
                      dest="no_monitoring",
                      default=False,
                      action="store_true",
                      )
    parser.add_option('-n', '--num-ec2-instances',
                      dest="number_of_ec2_instances",
                      default=7,
                      type="int",
                      )
    parser.add_option('-t', '--ec2-instance-type',
                      dest="ec2_instance_type",
                      default="t2.large",
                      type="string",
                      )
    parser.add_option('-s', '--sentry-disabled',
                      dest="sentry_disabled",
                      default=False,
                      action="store_true",
                      )
    parser.add_option('-c', '--testing',
                      dest="testing",
                      default=False,
                      action="store_true",
                      )
    parser.add_option('-o', '--operation',
                      dest="operation",
                      default="",
                      type="string"
                    )
    options, remainder = parser.parse_args()

    return options

def upload_csv_files_to_s3(db_name):
    s3 = boto3.client('s3',
                      aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                      aws_secret_access_key=conf.get('aws', 'aws_secret_access_key'),
                      region_name=conf.get('aws', 'region_name')
                      )
    tables = rds_handler.get_db_tables(db_name)
    try:
        for table in tables:
            tempfile = conf['csv']['path'] + table + ".csv"
            fil = open(tempfile, "rb")
            s3_path = "tipocaData/bckp_" + db_name +"_" + suffix + "/" + table
            response = s3.put_object(
                Bucket=conf.get('aws', 'bucket'),
                Key=s3_path,
                ACL="private",
                Body=fil
            )
    except botocore.exceptions.ClientError, err:
        print(err)