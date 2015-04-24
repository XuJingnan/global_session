#!/usr/bin/python

import sys

class IndexHourReduce(object):

    def __init__(self):
        self.previous_key = ''
        self.index_values = set()
        self.field_sep = '\0'
        self.input_field_number = 4
        self.index_output_table = 'udwetl_global_session_index'

    def __del__(self):
        self.output()

    # input format: index_key \t udwid \0 day \0 hour \0 product
    def reduce(self, line):
        tmp = line.strip().split('\t')
        if len(tmp) != 2:
            self.stderr_out('init_value', 'no tab:%s' % line)
            return -1
        key, value = tmp
        if len(value.split(self.field_sep)) != self.input_field_number:
            self.stderr_out('init_value', 'wrong field number')
            return -2
        if key != self.previous_key:
            self.output()
            self.previous_key = key
        self.index_values.add(value)

    # output format: index_key \t output_table_name \0 index_key \0 udwid \0 index_start_time \0 index_end_time \0
    # partition field:            day \0 hour \0 product
    def output(self):
        for v in self.index_values:
            udwid, day, hour, product = v.split(self.field_sep)
            index_start_time = day + hour
            prefix = self.field_sep.join([self.index_output_table, self.previous_key, udwid, index_start_time, index_start_time])
            suffix = self.field_sep.join([day, hour, product]) #partition: event_day, event_hour, event_product
            value = self.field_sep.join([prefix, suffix])
            print '\t'.join([self.previous_key, value])
        self.index_values.clear()

    def stderr_out(self, func_name, info, exit_code = 0):
        sys.stderr.write(func_name + ':\t' + info + '\n')
        if exit_code != 0:
            sys.exit(exit_code)

reduce = IndexHourReduce()

for line in sys.stdin:
    reduce.reduce(line)
