import math
import sys
import _mysql_exceptions
import boto3
import psycopg2
import rds_handler
import reserved

from app import config

conf = config.configuration
conn = boto3.client('rds',
                    aws_access_key_id=conf['aws']['aws_access_key_id'],
                    aws_secret_access_key=conf['aws']['aws_secret_access_key'],
                    region_name=conf['aws']['region_name'])


def get_redshift_conn(autocommit=True):
    redshift_conn = psycopg2.connect(
        database=conf["redshift"]["db"],
        user=conf["redshift"]["user"],
        password=conf["redshift"]["password"],
        host=conf["redshift"]["host"],
        port=conf["redshift"]["port"]
    )
    redshift_conn.autocommit = autocommit
    return redshift_conn


def close_redshift_conn(redshift_conn):
    redshift_conn.close()


def create_redshift_schema(db_name):
    include_tables = rds_handler.get_db_tables(db_name)
    print "Generating Schema DDS and Selector SQL Statements"
    try:
        rds_cursor = rds_handler.get_db_tables(db_name)
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
    redshift_reserved = reserved.redshift_reserved
    col_map = reserved.col_map
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
        dds_statements.append("CREATE SCHEMA " + db_name)
        for table_name, table_data in tables.items():
            dds_statements.append("CREATE TABLE " + db_name + "." \
                                  + table_name + " (" + ", ".join(table_data['col_defs']) + ")")
    except Exception, err:
        print err
        sys.exit(1)

    try:
        redshift_conn = get_redshift_conn()
        redshift_cursor = redshift_conn.cursor()
        for statement in dds_statements:
            print statement
            redshift_cursor.execute(statement)
        close_redshift_conn(redshift_conn)
    except psycopg2.Error, err:
        print err
        close_redshift_conn(redshift_conn)
        sys.exit(1)


def upload_csv_redshift(db_name):
    try:
        tables = rds_handler.get_db_tables(db_name)
        redshift_conn = get_redshift_conn()
        for table_name in tables:
            redshift_cursor = redshift_conn.cursor()
            sql = "COPY " + db_name + "." + table_name + " FROM " + \
                  "'s3://" + conf['aws']['bucket'] + conf['aws'][
                      's3_path'] + config.suffix + "/" + table_name + "' CREDENTIALS " + \
                  "'aws_access_key_id=" + conf['aws']['aws_access_key_id'] + \
                  ";aws_secret_access_key=" + conf['aws']['aws_secret_access_key'] + "' " + \
                  "FORMAT AS CSV QUOTE AS '\"' DELIMITER ',' EMPTYASNULL"
            print("Copying to Redshift table " + table_name)
            print sql
            redshift_cursor.execute(sql)
            redshift_cursor.close()
        close_redshift_conn(redshift_conn)
    except psycopg2.Error, err:
        print err
        close_redshift_conn(redshift_conn)
        sys.exit(1)


def update_data_using_binlog(db_name):
    redshift = psycopg2.connect(
        database=conf['redshift']['db'],
        user=conf['redshift']['user'],
        password=conf['redshift']['password'],
        host=conf['redshift']['host'],
        port=conf['redshift']['port'],
    )
    redshift.autocommit = True  # change this to differential commit
    redshift_cursor = redshift.cursor()
    redshift_cursor.execute("SET search_path TO '" + db_name + "'")
    with open('/Users/sandeep/Documents/project/code/experiments/tython/tmp/bin.log', 'rb') as f:
        for line in f:
            if (line.startswith("INSERT") or \
                        line.startswith("insert")) and 'mysql' not in line:
                redshift_cursor.execute(line)
