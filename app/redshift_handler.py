import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
import math
import psycopg2
import boto3
import config

tython_config = config.Config()
conf = tython_config.get_config()
conn = boto3.client('rds',
                    aws_access_key_id=conf.get('aws', 'aws_access_key_id'),
                    aws_secret_access_key=conf.get(
                        'aws', 'aws_secret_access_key'),
                    region_name=conf.get('aws', 'region_name'))
suffix = '1479893180'

tables = ["applicant_profile", "applicant_profile_version", "applicant_status", "applicants",
                  "applicants_version", "candidate_metadata", "companies", "cv_status", "degrees", "feature_request",
                  "interview_details", "interview_details_version", "interview_status", "interview_type",
                  "interviewer_interview_relations", "interviewer_status", "load_data", "project_requisition_relations",
                  "projects", "requisition", "roles", "rounds", "sources", "test", "transaction", "universities",
                  "user_requisition_relations", "users"]

class RedshiftHandler:
    def create_redshift_schema(object):
        include_tables = ["applicant_profile", "applicant_profile_version", "applicant_status", "applicants",
                          "applicants_version", "candidate_metadata", "companies", "cv_status", "degrees",
                          "feature_request", "interview_details", "interview_details_version", "interview_status",
                          "interview_type", "interviewer_interview_relations", "interviewer_status", "load_data",
                          "project_requisition_relations", "projects", "requisition", "roles", "rounds", "sources",
                          "test", "transaction", "universities", "user_requisition_relations", "users"]
        schema_name = "hireninja"
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
            print(err)
            sys.exit(1)

        try:
            result = rds_cursor.fetchall()
        except _mysql_exceptions.Error, err:
            print err
            sys.exit(1)
        dds_statements = []
        select_statements = []
        redshift_reserved = [
            "AES128",
            "AES256",
            "ALL",
            "ALLOWOVERWRITE",
            "ANALYSE",
            "ANALYZE",
            "AND",
            "ANY",
            "ARRAY",
            "AS",
            "ASC",
            "AUTHORIZATION",
            "BACKUP",
            "BETWEEN",
            "BINARY",
            "BLANKSASNULL",
            "BOTH",
            "BYTEDICT",
            "CASE",
            "CAST",
            "CHECK",
            "COLLATE",
            "COLUMN",
            "CONSTRAINT",
            "CREATE",
            "CREDENTIALS",
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "CURRENT_USER_ID",
            "DEFAULT",
            "DEFERRABLE",
            "DEFLATE",
            "DEFRAG",
            "DELTA",
            "DELTA32K",
            "DESC",
            "DISABLE",
            "DISTINCT",
            "DO",
            "ELSE",
            "EMPTYASNULL",
            "ENABLE",
            "ENCODE",
            "ENCRYPT",
            "ENCRYPTION",
            "END",
            "EXCEPT",
            "EXPLICIT",
            "FALSE",
            "FOR",
            "FOREIGN",
            "FREEZE",
            "FROM",
            "FULL",
            "GLOBALDICT256",
            "GLOBALDICT64K",
            "GRANT",
            "GROUP",
            "GZIP",
            "HAVING",
            "IDENTITY",
            "IGNORE",
            "ILIKE",
            "IN",
            "INITIALLY",
            "INNER",
            "INTERSECT",
            "INTO",
            "IS",
            "ISNULL",
            "JOIN",
            "LEADING",
            "LEFT",
            "LIKE",
            "LIMIT",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LUN",
            "LUNS",
            "LZO",
            "LZOP",
            "MINUS",
            "MOSTLY13",
            "MOSTLY32",
            "MOSTLY8",
            "NATURAL",
            "NEW",
            "NOT",
            "NOTNULL",
            "NULL",
            "NULLS",
            "OFF",
            "OFFLINE",
            "OFFSET",
            "OLD",
            "ON",
            "ONLY",
            "OPEN",
            "OR",
            "ORDER",
            "OUTER",
            "OVERLAPS",
            "PARALLEL",
            "PARTITION",
            "PERCENT",
            "PERMISSIONS",
            "PLACING",
            "PRIMARY",
            "RAW",
            "READRATIO",
            "RECOVER",
            "REFERENCES",
            "REJECTLOG",
            "RESORT",
            "RESTORE",
            "RIGHT",
            "SELECT",
            "SESSION_USER",
            "SIMILAR",
            "SOME",
            "SYSDATE",
            "SYSTEM",
            "TABLE",
            "TAG",
            "TDES",
            "TEXT255",
            "TEXT32K",
            "THEN",
            "TO",
            "TOP",
            "TRAILING",
            "TRUE",
            "TRUNCATECOLUMNS",
            "UNION",
            "UNIQUE",
            "USER",
            "USING",
            "VERBOSE",
            "WALLET",
            "WHEN",
            "WHERE",
            "WITH",
            "WITHOUT"
        ]
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
            "POLYGON": "UNSUPPORTED",
        }
        tables = {}
        for row in result:
            table_name = row['TABLE_NAME']
            col_type = col_map.get(row['DATA_TYPE'].upper())
            if not col_type:
                print ("Skipping unknown column type %s for %s.%s" % (
                    row['DATA_TYPE'],
                    row['TABLE_NAME'],
                    row['COLUMN_NAME']
                ))
                continue
            if col_type == 'UNSUPPORTED':
                continue
            col_name = row['COLUMN_NAME']
            if col_name.upper() in redshift_reserved:
                col_def = table_name + "_" + col_name + " " + col_type
            else:
                col_def = col_name + " " + col_type
            if table_name not in include_tables:
                continue

            batch_size = 0
            if table_name not in tables:
                tables[table_name] = {
                    "primary_key": None,
                    "col_defs": [],
                    "row_count": row['TABLE_ROWS'],
                    "table_size_mb": row['T_SIZE'],
                    "batch_size": batch_size,
                    "row_size": row['R_SIZE'],
                    "min": None,
                    "max": None
                }
            if col_type == "VARCHAR":
                char_length = row['CHARACTER_MAXIMUM_LENGTH']
                if row['DATA_TYPE'].upper() == "TIME":
                    char_length = 8
                if not char_length:
                    char_length = 32768
                char_length = math.ceil(char_length * 4)
                if (char_length > 32768):
                    char_length = 32768
                col_def += "(" + str(int(char_length)) + ")"
            if col_type == "DECIMAL":
                col_def += "(" + str(row['NUMERIC_PRECISION']) + ", " + str(row['NUMERIC_SCALE']) + ")"
            tables[table_name]['col_defs'].append(col_def)
        try:
            dds_statements.append("CREATE SCHEMA " + schema_name)
            for table_name, table_data in tables.items():
                dds_statements.append("CREATE TABLE " + schema_name + "." \
                                      + table_name + " (" + ", ".join(table_data['col_defs']) + ")")
        except Exception, err:
            print err
            sys.exit(1)

        try:
            redshift = psycopg2.connect(
                database=conf.get("redshift", "db"),
                user=conf.get("redshift", "user"),
                password=conf.get("redshift", "password"),
                host=conf.get("redshift", "host"),
                port=conf.get("redshift", "port")
            )
            redshift.autocommit = True
            redshift_cursor = redshift.cursor()
            for statement in dds_statements:
                print statement
                redshift_cursor.execute(statement)
        except psycopg2.Error, err:
            print err
            sys.exit(1)

    def upload_csv_redshift(self):
        try:
            redshift = psycopg2.connect(
                database=conf.get('redshift','db'),
                user=conf.get('redshift','user'),
                password=conf.get('redshift','password'),
                host=conf.get('redshift','host'),
                port=conf.get('redshift','port'),
            )
            redshift.autocommit = True
            #truncate_table_query = "truncate " + db_name + "_" + suffix + "." + table_name
            # truncate table to avoid duplicates
            db_name = conf.get('hireninja', 'db')
            #redshift_cursor.execute(truncate_table_query)
            for table_name in tables:
                redshift_cursor = redshift.cursor()
                sql = "COPY " + db_name + "." + table_name + " FROM " + \
                      "'s3://" + conf.get('aws','bucket') + "/tipocaData/bckp_" + suffix + "/" + table_name + "' CREDENTIALS " + \
                      "'aws_access_key_id=" + conf.get('aws','aws_access_key_id') + \
                      ";aws_secret_access_key=" + conf.get('aws','aws_secret_access_key') + "' " + \
                      "FORMAT AS CSV QUOTE AS '\"' DELIMITER ',' EMPTYASNULL"
                print("Copying to Redshift table "+ table_name)
                print sql
                redshift_cursor.execute(sql)
                redshift_cursor.close()
        except psycopg2.Error, err:
            print err
            sys.exit(1)
        redshift.close()

    def update_data_using_binlog(self):
        redshift = psycopg2.connect(
            database=conf.get('redshift', 'db'),
            user=conf.get('redshift', 'user'),
            password=conf.get('redshift', 'password'),
            host=conf.get('redshift', 'host'),
            port=conf.get('redshift', 'port'),
        )
        redshift.autocommit = True
        redshift_cursor = redshift.cursor()
        redshift_cursor.execute("SET search_path TO 'hireninja'")
        with open('/Users/sandeep/Documents/project/code/experiments/tython/tmp/bin.log', 'rb') as f:
            for line in f:
                if (line.startswith("INSERT") or \
                        line.startswith("insert")) and 'mysql' not in line:
                    redshift_cursor.execute(line)