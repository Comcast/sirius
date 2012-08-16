#!/usr/bin/env python

import base64
import getopt
import hashlib
import sys
import collections
import sqlite3

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from unidecode import unidecode

from core_data.BobMerlinMappingProto_pb2 import BobMerlinMappingMessage
from core_data.ProgramProto_pb2 import ProgramMessage
from core_data.VodProto_pb2 import VodAssetMessage
protobuf_types = {"vod":VodAssetMessage,
                  "program":ProgramMessage,
                  "mapping":BobMerlinMappingMessage
                  }

col_types = {3:["TYPE_INT64", 'integer'],
             5:["TYPE_INT32", 'integer'],
             8:["TYPE_BOOL", 'integer'],
             9:["TYPE_STRING", 'text']
             }
VERSION = '0.1.0'

def usage(f):
    print """usage: wal [-?hnmf] [-i <file>] [-o <file>] <command>
  -?,-h,--help     : get usage message
  -n,--no-checksum : skip checksum verification
  -f,--fnv-1a      : use FNV-1a checksum algorithm (default)
  -m,--md5         : use MD5 checksum algorithm
  -i,--input-file  : read log from file rather than stdin
  -o,--output-file : write modified log to file rather than stdout
  <command>        : one of : check, repair

COMMANDS:
  check  : read the log and verify its integrity
  repair : output the longest prefix of the log that is verified good

EXAMPLES:
  $ wal check < wal.log
  $ wal repair < corrupted.log > repaired.log"""

def check_entry_skip(n, entry): return True


def check_entry_md5(n, entry):
    first_pipe = entry.index('|')
    m = hashlib.md5()
    m.update(entry[first_pipe:])
    return entry[:first_pipe] == base64.b64encode(m.digest())


def check_entry_fnv_1a(n, entry):
    first_pipe = entry.index('|')
    h = 14695981039346656037
    for c in entry[first_pipe:]:
        h ^= ord(c)
        h *= 1099511628211
        h &= 0xffffffffffffffff
    return entry[:first_pipe] == ("%016x" % h)


def read_protobuf_body(line_type, body, protobuf_types):
    """Returns a protocol buffer message of the correct
    type based upon the record_type argument and contents
    of the body.
    """
    message = None
    if line_type in protobuf_types:
        message = protobuf_types[line_type]()
        decoded_body = base64.b64decode(body)
        message.ParseFromString(decoded_body)
    return message


def decode_list(data):
    """Returns a list of data unidecoded."""
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        rv.append(item)
    return rv


def decode_dict(data):
    """Returns a dictionary unidecoded."""
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = unidecode(value)
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = decode_list(value)
        elif isinstance(value, dict):
            value = decode_dict(value)
        if value == None:
            value = ''
        rv[key] = value
    return rv


def parse_log_line(log_line):
    """Create a dictionary from the log line."""
    line_elements = log_line.split("|")
    line = {}
    line['id'] = line_elements[0]
    line['action'] = line_elements[1]
    line['key'] = line_elements[2]
    line['seq'] = line_elements[3]
    line['date'] = line_elements[4]
    line['line_type'] = line['key'].split("/")[0]
    line['body'] = line_elements[5]
    return line


def build_insert_statement(protobuf_name, table_row):
    """Creates insert sql statements."""
    insert_template = "INSERT INTO {0} ({1}) VALUES ({2})"
    sql = insert_template.format(protobuf_name,
                                 ', '.join([column_name for column_name
                                            in table_row.keys()]),
                                 ', '.join(['?' for x in table_row.keys()]))
    return sql


def build_drop_statement(protobuf_name):
    """Creates the drop table sql statement."""
    ddl = "DROP TABLE IF EXISTS {0}".format(protobuf_name)
    return ddl


def build_create_statement(protobuf_name, protobuf_fields, col_types):
    """Creates the create table sql statement."""
    ddl = "CREATE TABLE {0} (".format(protobuf_name)
    for idx, field in enumerate(protobuf_fields):
        sql_col_type = col_types[field.type][1]
        ddl = ddl + field.name + " " + sql_col_type
        if idx < len(protobuf_fields)-1:
            ddl = ddl + ", "
    ddl = ddl + ")"
    return ddl


def is_table_creatable(protobuf_name, protobuf_fields, col_types):
    """Determines if a protobuf definition has sqlite column
    types understood by this script.
    """
    for field in protobuf_fields:
        if field.type not in col_types:
            sys.stderr.write("%s has a field not defined for sqlite",
                              protobuf_name)
            return False
    return True


def create_db(db):
    """Creates a sqlite database schema at the location
    indicated in the db argument. Each table's
    definition determined by each protocol buffer's
    DESCRIPTOR.

    It returns the cursor and connection used to create the db.
    """
    connection = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = connection.cursor()

    for protobuf_type in protobuf_types.values():
        protobuf_name =  protobuf_type.DESCRIPTOR.name
        protobuf_fields = protobuf_type.DESCRIPTOR.fields
        if is_table_creatable(protobuf_name,
                              protobuf_fields,
                              col_types):
            ddl_statement = build_drop_statement(protobuf_name)
            cursor.execute(ddl_statement)

            ddl_statement = build_create_statement(protobuf_name,
                                             protobuf_fields,
                                             col_types)
            cursor.execute(ddl_statement)
    connection.commit()
    return connection, cursor


def insert_row(connection, cursor, log_line):
    """Will insert a log_line (transformed into a protobuffer)
    into the database via the connection and cursor arguments.

    This method does not commit the transaction!

    This enables the caller to batch up inserts
    and execute a commit after n-number of executions.
    """
    line = parse_log_line(log_line)
    message = read_protobuf_body(line['line_type'],
                                 line['body'],
                                 protobuf_types)
    table_name = message.DESCRIPTOR.name

    # create a filtered dictionary of col/vals from the protobuf
    # containing only those with a value, with that value decoded
    # and prepared for executing within a sql statement.
    table_row = collections.OrderedDict()
    for idx, field in enumerate(message.DESCRIPTOR.fields):
        if message.HasField(field.name):
            field_data = decode_dict({'field_name':field.name,
                                      'field_value':message.__getattribute__(field.name)})
            padded_value = field_data['field_value']
            if col_types[field.type][0] == "TYPE_STRING":
                padded_value = "\"" + field_data['field_value'] + "\""
            if col_types[field.type][0] == "TYPE_BOOL":
                padded_value = int(field_data['field_value'])

            table_row[field.name] = padded_value

    sql = build_insert_statement(table_name, table_row)
    cursor.execute(sql, table_row.values())
    return message


def check_repair(f, outf, db, checksum):
    entries = 0
    truncated = 0
    missing_fields = 0
    corrupted = 0

    import time
    start = time.time()


    if db:
        connection, cursor = create_db(db)

    for line in f:
        ok = True
        if not line.endswith('\n'):
            sys.stderr.write("entry %d not terminated by newline\n" % entries)
            truncated += 1
            ok = False
        elif not len(line.split('|')) == 6:
            sys.stderr.write("entry %d does not have 6 pipe-delimited fields\n" %
                             entries)
            missing_fields += 1
            ok = False
        elif not checksum(entries, line):
            sys.stderr.write("entry %d has bad checksum\n" % entries)
            corrupted += 1
            ok = False
        elif outf is not None: outf.write(line)

        if not ok and outf is not None:
            outf.flush()
            outf = None

        if ok and db:
            message = insert_row(connection, cursor, line)
            if entries % 100000 == 0:
                connection.commit()
        entries += 1
        if entries % 100000 == 0:
            sys.stderr.write("wal: %d entries in %d\n" % (entries, time.time()-start))

    if db:
        connection.commit()
        connection.close()

    if outf is not None:
        outf.flush()

    sys.stderr.write("wal: %d total entries\n" % entries)
    if truncated > 0:
        sys.stderr.write("wal: %d truncated entries\n" % truncated)
    if missing_fields > 0:
        sys.stderr.write("wal: %d entries with missing fields\n" % missing_fields)
    if corrupted > 0:
        sys.stderr.write("wal: %d entries with bad checksums\n" % corrupted)
    sys.stderr.flush()

if __name__ == "__main__":
    check_checksums = True
    checksum = check_entry_fnv_1a
    db = None
    output_file = None

    import optparse
    op = optparse.OptionParser(usage='%prog [options]',
                               version='%prog ' + VERSION)

    op.add_option('-n', help='Run with no checksum verification.',
                  dest='check_checksums', action='store_true', default=False)

    op.add_option('-m', help='Enables md5 for checksums.',
                  dest='use_md5', action='store_true', default=False)

    op.add_option('-d', help='The location of a database to generate.',
                  dest='db', action='store', type='string')

    op.add_option('-o', help='Location to store a repaired file.',
                  dest='output_file', action='store', type='string')

    op.add_option('-i', help='Location of the WAL file.',
                  dest='input_file', action='store', type='string')

    op.add_option('-l', help='A log line to parse.',
                  dest='log_line', action='store', type='string')

    op.add_option('-p', help='Pipe a log line with this flag enabled',
                  dest='pipe_log', action='store_true', default=None)

    (opts, args) = op.parse_args()

    if opts.use_md5:
        checksum = check_entry_md5

    lines = []
    if opts.log_line:
        line = parse_log_line(opts.log_line)
        lines.append(line)
    elif opts.pipe_log:
        for log_line in sys.stdin.readlines():
            lines.append(parse_log_line(log_line))
    if opts.log_line or opts.pipe_log:
        for line in lines:
            message = read_protobuf_body(line['line_type'],
                                         line['body'],
                                         protobuf_types)
            sys.stdout.write("---\n" + str(message))

    if opts.output_file:
        try:
            output_file = open(opts.output_file,"w")
        except:
            sys.stderr.write("%s\n" % sys.exc_info()[1])
            sys.exit(1)

    if opts.input_file:
        if opts.input_file == opts.output_file:
            sys.stderr.write('Input and Output files are the same!')
            sys.exit(1)
        try:
            input_file = open(opts.input_file)
        except:
            sys.stderr.write("%s\n" % sys.exc_info()[1])
            sys.exit(1)

        check_repair(input_file, output_file, opts.db, checksum)

    if opts.output_file:
        output_file.close()
