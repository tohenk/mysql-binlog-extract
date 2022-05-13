#!/bin/env python3

# ----------------------------------
# MySQL Bin Log SQL Extractor
# Extract bin log for specific table
# (c) 2022 Toha <tohenk@yahoo.com>
#
# Last modified: April 22, 2022
# ----------------------------------

import re
import sys
from os.path import basename, exists, splitext

class BinlogExtractor:
    dbg = False
    handle = None
    filename = None
    outfilename = None
    tables = []

    def __init__(self, filename: str, tables: list) -> None:
        self.filename = filename
        self.tables = tables

    def extract(self) -> None:
        if not exists(self.filename):
            print('File not found %s!' % self.filename)
            return
        self.handle = None
        delimiter = '/*!*/;'
        binlog = None
        header = None
        headers = []
        prev = None
        collected = False
        fqtablename = None
        with open(self.filename, 'r') as f:
            for line in f:
                # parse for headers
                if header is None:
                    # check for delimeter
                    matches = re.search(r'^DELIMITER\s+(?P<DELIM>.*)', line)
                    if matches is not None:
                        delimiter = matches.group('DELIM')
                        self.log('DELIMITER set to %s...' % delimiter)
                    # check for binlog
                    if binlog is None and line.find('BINLOG') >= 0:
                        binlog = True
                        self.debug('Found BINLOG header...')
                    # check for binlog end
                    if binlog and line.find(delimiter) >= 0:
                        header = True
                        self.debug('BINLOG header completed...')
                    headers.append(line)
                else:
                    # find operation
                    matches = re.search((
                        r'^#(?P<DATE>\d{6})\s+'
                        r'(?P<TIME>\d{1,2}\:\d{2}\:\d{2})\s+'
                        r'server id\s+(?P<SID>\d+)\s+'
                        r'end_log_pos\s+(?P<END_LOG_POS>\d+)\s+'
                        r'CRC32\s+(?P<CRC32>0x[a-z0-9]+)\s+'
                        r'(?P<TYPE>[A-Za-z\_]+\:?)\s+(?P<EXTRA>.*)'
                        ), line)
                    # is it Query?
                    if matches is not None and matches.group('TYPE') == 'Query':
                        collected = True
                        table = None
                        matched = None
                        lines = [prev]
                        found = 0
                    # Collect lines if a query
                    if collected:
                        # check for table
                        if matches is not None and matches.group('TYPE') == 'Table_map:':
                            table = True
                            params = matches.group('EXTRA').split()
                            fqtablename = params[0]
                            dbname, tablename = fqtablename.split('.')
                            dbname = self.unquote(dbname)
                            tablename = self.unquote(tablename)
                            matched = tablename in self.tables
                            if matched:
                                found += 1
                                self.log('Found match for table %s...' % fqtablename)
                            else:
                                lines.pop()
                                self.debug('Table is excluded %s...' % fqtablename)
                        # check for end of operation
                        if line.find('COMMIT') >= 0:
                            collected = False
                        if table is None or matched is True:
                            lines.append(line)
                        # check for end table
                        if table is True and line.find(delimiter) >= 0:
                            table = None
                        if not collected and found > 0:
                            if self.handle is None:
                                self.write_sql(headers)
                            self.write_sql(lines)
                    else:
                        prev = line

    def unquote(self, identifier: str) -> str:
        return identifier.strip('`')
    
    def write_sql(self, lines: list) -> None:
        if self.handle is None:
            if self.outfilename is None:
                filename, ext = splitext(self.filename)
                self.outfilename = filename + '-extracted' + ext
            self.handle = open(self.outfilename, 'w')
        self.handle.writelines(lines)

    def log(self, message: str) -> None:
        print(message)

    def debug(self, message: str) -> None:
        if self.dbg:
            print(message)
        pass

if __name__ == '__main__':
    arguments = sys.argv
    name = basename(arguments.pop(0))
    debug = False
    filename = None
    outfilename = None
    tables = []
    while len(arguments) > 0:
        x = arguments.pop(0)
        if (x == '--table' or x == '-t') and len(arguments) > 0:
            tables.append(arguments.pop(0))
            continue
        if (x == '--out' or x == '-o') and len(arguments) > 0:
            outfilename = arguments.pop(0)
            continue
        if (x == '--debug' or x == '-d'):
            debug = True
            continue
        filename = x
        break
    if len(arguments) > 0 or filename is None:
        print('Usage: %s [options] SQL-FILE' % name)
        print('')
        print('SQL-FILE should be SQL file generated by mysqlbinlog program.')
        print('')
        print('Options:')
        print('-t, --table tablename    Specify tablename to be extracted')
        print('                         Can be supplied multiple times to extract more tables')
        print('-o, --out filename       Specify out filename to write to')
        print('-d, --debug              Turn on debugging')
        print('')
        print('Example:')
        print('%s -t mytable /path/to/my/file' % name)
        print('')
        exit()
    
    extractor = BinlogExtractor(filename, tables)
    if outfilename is not None:
        extractor.outfilename = outfilename
    if debug:
        extractor.dbg = True
    extractor.extract()