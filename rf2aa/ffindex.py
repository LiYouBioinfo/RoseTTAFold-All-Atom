#!/usr/bin/env python
# https://raw.githubusercontent.com/ahcm/ffindex/master/python/ffindex.py

'''
Created on Apr 30, 2014

@author: meiermark
'''


import sys
import mmap    # mmap: Memory-mapped file objects allow Python to access the content of a file directly in memory, which is useful for efficient file reading and writing.
from collections import namedtuple

# It is like initiating a class named FFindexEntry in java, with name, offset, and length as its build-in variables. FFindexEntry ffentry = new FFindexEntry(String name, Object offset, int length); 
FFindexEntry = namedtuple("FFindexEntry", "name, offset, length")
def read_index(ffindex_filename):
    entries = []
    
    fh = open(ffindex_filename, "r")
    for line in fh:
        tokens = line.split("\t")
        entries.append(FFindexEntry(tokens[0], int(tokens[1]), int(tokens[2])))
    fh.close()
    
    return entries

# Similar to Java NIO package. It can access particular position more efficiently without opening the whole file in to memory. 
def read_data(ffdata_filename):
    # rb means read the file in binary read mode. 
    fh = open(ffdata_filename, "rb")
    # fh.fileno() retrieves file descriptor from the file handle (an integer). 0 indicates teh size of the mapping. setting to 0 means the entire file will be mapped. 
    data = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
    fh.close()
    return data


def get_entry_by_name(name, index):
    #TODO: bsearch
    for entry in index:
        if(name == entry.name):
            return entry
    return None


def read_entry_lines(entry, data):
    # slice the memory-mapped file `data` using data[start:end] syntax, then decode by utf-8 and split by newline char.
    lines = data[entry.offset:entry.offset + entry.length - 1].decode("utf-8").split("\n")
    return lines

# similar to the command above. But it returns the byte slice, instead of Strings in UTF-8 encoding. 
def read_entry_data(entry, data):
    return data[entry.offset:entry.offset + entry.length - 1]


def write_entry(entries, data_fh, entry_name, offset, data):
    data_fh.write(data[:-1])
    data_fh.write(bytearray(1))

    entry = FFindexEntry(entry_name, offset, len(data))
    entries.append(entry)

    return offset + len(data)


def write_entry_with_file(entries, data_fh, entry_name, offset, file_name):
    with open(file_name, "rb") as fh:
        data = bytearray(fh.read())
        return write_entry(entries, data_fh, entry_name, offset, data)


def finish_db(entries, ffindex_filename, data_fh):
    data_fh.close()
    write_entries_to_db(entries, ffindex_filename)


def write_entries_to_db(entries, ffindex_filename):
    sorted(entries, key=lambda x: x.name)
    index_fh = open(ffindex_filename, "w")

    for entry in entries:
        index_fh.write("{name:.64}\t{offset}\t{length}\n".format(name=entry.name, offset=entry.offset, length=entry.length))

    index_fh.close()


def write_entry_to_file(entry, data, file):
    lines = read_lines(entry, data)

    fh = open(file, "w")
    for line in lines:
        fh.write(line+"\n")
    fh.close()
