##
# Class including all the required methods for handling rds related operations
##

import boto3
import config
import time
import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys

tython_config = config.Config()
conf = tython_config.get_config()
conn = boto3.client('rds',
                    aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                    aws_secret_access_key=conf.get(
                        'aws', 'aws_secret_access_key'),
                    region_name=conf.get('aws', 'region_name'))
suffix = '1479893180'


class RdsHandler:

    def get_snapshot_list(object):
        snapshots = conn.describe_db_snapshots(
            DBInstanceIdentifier=conf.get('aws', 'databases'),
            MaxRecords=100
        )['DBSnapshots']
        snapshots = filter(lambda x: x.get('SnapshotCreateTime'), snapshots)
        latest_snapshot = max(snapshots, key=lambda s: s['SnapshotCreateTime'])
        return latest_snapshot

    def create_rds_instance_from_snapshot(object, latest_snapshot):
        db_instance_identifier = 'tython-' + conf.get('aws', 'databases') + \
            "-" + suffix
        response = conn.restore_db_instance_from_db_snapshot(
            DBSnapshotIdentifier=latest_snapshot['DBSnapshotIdentifier'],
            DBInstanceIdentifier=db_instance_identifier,
            PubliclyAccessible=True,
            MultiAZ=False
        )
        print response
        #{u'DBInstance': {u'PubliclyAccessible': True, u'MasterUsername': 'hireninjabuilder', u'MonitoringInterval': 0, u'LicenseModel': 'general-public-license', u'VpcSecurityGroups': [{u'Status': 'active', u'VpcSecurityGroupId': 'sg-62110200'}], u'CopyTagsToSnapshot': False, u'OptionGroupMemberships': [{u'Status': 'pending-apply', u'OptionGroupName': 'default:mysql-5-6'}], u'PendingModifiedValues': {}, u'Engine': 'mysql', u'MultiAZ': False, u'DBSecurityGroups': [], u'DBParameterGroups': [{u'DBParameterGroupName': 'default.mysql5.6', u'ParameterApplyStatus': 'in-sync'}], u'AutoMinorVersionUpgrade': True, u'PreferredBackupWindow': '18:48-19:18', u'DBSubnetGroup': {u'Subnets': [{u'SubnetStatus': 'Active', u'SubnetIdentifier': 'subnet-496d7d2b', u'SubnetAvailabilityZone': {u'Name': 'ap-southeast-1b'}}, {u'SubnetStatus': 'Active', u'SubnetIdentifier': 'subnet-f4dcec80', u'SubnetAvailabilityZone': {u'Name': 'ap-southeast-1a'}}], u'DBSubnetGroupName': 'default', u'VpcId': 'vpc-11d4c373', u'DBSubnetGroupDescription': 'default', u'SubnetGroupStatus': 'Complete'}, u'ReadReplicaDBInstanceIdentifiers': [], u'AllocatedStorage': 5, u'DBInstanceArn': 'arn:aws:rds:ap-southeast-1:754922593538:db:tython-hireninja-1479893180', u'BackupRetentionPeriod': 1, u'DBName': 'hireninja', u'PreferredMaintenanceWindow': 'sun:14:54-sun:15:24', u'DBInstanceStatus': 'creating', u'EngineVersion': '5.6.23', u'DomainMemberships': [], u'StorageType': 'gp2', u'DbiResourceId': 'db-P6D2EP3I6NCQN6ASYKUNQLSCKI', u'CACertificateIdentifier': 'rds-ca-2015', u'StorageEncrypted': False, u'DBInstanceClass': 'db.m1.small', u'DbInstancePort': 0, u'DBInstanceIdentifier': 'tython-hireninja-1479893180'}, 'ResponseMetadata': {'RetryAttempts': 0, 'HTTPStatusCode': 200, 'RequestId': '68417af1-b15f-11e6-a094-49e1e0014924', 'HTTPHeaders': {'x-amzn-requestid': '68417af1-b15f-11e6-a094-49e1e0014924', 'vary': 'Accept-Encoding', 'content-length': '3376', 'content-type': 'text/xml', 'date': 'Wed, 23 Nov 2016 09:30:03 GMT'}}}

    def create_redshift_schema(object):
    	col_map = {
                "INTEGER": "INTEGER",
                "INT": "INTEGER",
                "TINYINT": "SMALLINT",
                "SMALLINT": "SMALLINT",
                "MEDIUMINT": "INTEGER",
                "BIGINT": "BIGINT",
                "DECIMAL": "DECIMAL",
                "NUMERIC": "DECIMAL",
                "FLOAT": "FLOAT",
                "DOUBLE": "FLOAT",
                "DATE": "DATE",
                "TIME": "VARCHAR",
                "YEAR": "SMALLINT",
                "DATETIME": "TIMESTAMP",
                "TIMESTAMP": "TIMESTAMP",
                "CHAR": "VARCHAR",
                "VARCHAR": "VARCHAR",
                "BINARY": "VARCHAR",
                "VARBINARY": "VARCHAR",
                "BLOB": "VARCHAR",
                "TINYBLOB": "VARCHAR",
                "MEDIUMBLOB": "VARCHAR",
                "LONGBLOB": "VARCHAR",
                "TEXT": "VARCHAR",
                "TINYTEXT": "VARCHAR",
                "MEDIUMTEXT": "VARCHAR",
                "LONGTEXT": "VARCHAR",
                "ENUM": "VARCHAR",
                "SET": "VARCHAR",
                "BIT": "VARCHAR",
                "POLYGON": "UNSUPPORTED"
                }
        print "Generating Schema DDS and Selector SQL Statements"
        try:
            rds = MySQLdb.connect(
            	user=conf.get('hireninja', 'user'),
            	passwd=conf.get('hireninja', 'password'),
            	host=conf.get('hireninja', 'host'),
            	port=int(conf.get('hireninja', 'port')),
            	db=conf.get('hireninja', 'db'),
            	cursorclass=MySQLdb.cursors.DictCursor)
            rds_cursor = rds.cursor()
            rds_cursor.execute("SELECT c.*, t.TABLE_ROWS, " +
                                       "ceil(((t.data_length+t.index_length)/(1024*1024))) as T_SIZE, " +
                                       "if(t.TABLE_ROWS > 0,ceil(ceil(((t.data_length+t.index_length)))/t.TABLE_ROWS),0) as R_SIZE FROM " +
                                       "information_schema.columns c LEFT JOIN information_schema.tables t " +
                                       "ON t.table_name = c.table_name and t.table_schema = c.table_schema " +
                                       "WHERE t.TABLE_TYPE='BASE TABLE' AND c.table_schema = '" + conf.get('hireninja', 'db') +
                                       "' order by c.table_name, c.ordinal_position")
        except _mysql_exceptions.Error, err:
        	print (err)
        	sys.exit(1)
        try:
        	result = rds_cursor.fetchall()
        except _mysql_exceptions.Error, err:
        	logger.error(err, exc_info=True)
        	sys.exit(1)
        print result
