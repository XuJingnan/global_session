#!/usr/bin/python

import sys

class MergeReducer(object):

    def __init__(self):
        self.field_sep = '\0'
        self.data_table_name = 'udwetl_global_session_data_merge'
        self.index_table_name = 'udwetl_global_session_index_merge'
        # input format: index_key \t output_table_name \0 index_key \0 udwid \0 index_start_time \0 index_end_time \0
        # partition field:           day \0 hour \0 product
        self.values_number = 8
        self.previous_key = ''
        self.map = dict()

    def init_value(self, line):
        line = line.strip()
        if line.find('\t') == -1:
            self.stderr_out('init_value', 'no tab:%s' % line)
            return -1
        key, values = line.split('\t')
        values = values.split(self.field_sep)
        if line.find(self.index_table_name) != -1 and len(values) < self.values_number:
            self.stderr_out('init_value', 'wrong field number for index data')
            return -2
        self.table_name = values[0] # table name
        if self.table_name == self.data_table_name:
            print '\t'.join([key, self.field_sep.join(values[:-2])])
            return -3 
        elif self.table_name != self.index_table_name:
            return -4
        self.index_key, self.udwid, self.start_time, self.end_time, self.day = values[1:-2]
        return 0

    def stderr_out(self, func_name, info, exit_code = 0):
        sys.stderr.write(func_name + ':\t' + info + '\n')
        if exit_code != 0:
            sys.exit(exit_code)

    def reduce(self):
        if self.index_key != self.previous_key:
            self.output()
            self.previous_key = self.index_key
        if self.udwid not in self.map:
            self.map[self.udwid] = [self.start_time, self.end_time]
        else:
            if self.start_time < self.map[self.udwid][0]:
                self.map[self.udwid][0] = self.start_time
            if self.end_time > self.map[self.udwid][1]:
                self.map[self.udwid][1] = self.end_time

    # output format: index_key \t tablename \0 index_key \0 udwid \0 start_time \0 end_time \0
    # partition:                  day
    def output(self):
        for udwid in self.map:
            value = self.field_sep.join([self.table_name, self.previous_key, udwid, self.map[udwid][0], self.map[udwid][1], self.day])
            print '\t'.join([self.previous_key, value])
        self.map.clear()

    def __del__(self):
        self.output()

reduce = MergeReducer()

for line in sys.stdin:
    res = reduce.init_value(line)
    if res < 0:
        continue
    reduce.reduce()
