#!/usr/bin/python

import sys

class MergeMapper(object):

    def __init__(self):
        self.field_sep = '\0'
        self.count = 0
        self.previous_key = ''
        self.data_table_name = 'udwetl_global_session_data'
        self.max_data_num = 500

    def init_value(self, line):
        line = line.strip()
        if line.find('\t') == -1:
            self.stderr_out('init_value', 'no tab')
            return -1
        values = line.split('\t')[1]
        self.values = values.split(self.field_sep)
        if len(self.values) <= 2:
            self.stderr_out('init_value', 'wrong field number')
            return -2
        self.tablename = self.values[0]
        self.key = self.values[1]  # map_key
        return 0

    def stderr_out(self, func_name, info, exit_code = 0):
        sys.stderr.write(func_name + ':\t' + info + '\n')
        if exit_code != 0:
            sys.exit(exit_code)

    def map(self):
        if self.key != self.previous_key:
            self.values[0] += '_merge'  # table name
            print '\t'.join([self.key, self.field_sep.join(self.values)])
            self.previous_key = self.key
            if self.tablename == self.data_table_name:
                self.count = 1
        else:
            if self.tablename == self.data_table_name and self.count == self.max_data_num:
                return 
            self.values[0] += '_merge'  # table name
            print '\t'.join([self.key, self.field_sep.join(self.values)])
            if self.tablename == self.data_table_name:
                self.count += 1
            
mapper = MergeMapper()

for line in sys.stdin:
    res = mapper.init_value(line)
    if res < 0:
        continue
    mapper.map()
