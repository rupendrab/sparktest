#!/usr/bin/env python

import sys
import os
from os.path import basename
import time

def copy_file(filename, outdir):
    outfile = outdir + "/" + basename(filename)
    tempfile = outfile + "__temp"
    fout = open(tempfile, 'w')
    lines = 0
    for line in open(filename, 'r'):
        lines += 1
        fout.write(line)
    fout.close()
    os.rename(tempfile, outfile)


if __name__ == '__main__':
    if (len(sys.argv) != 3):
        print("Usage: %s <File> <Output Directory>" % (sys.argv[0]))
        sys.exit(1)
    filename = sys.argv[1]
    outdir = sys.argv[2]
    copy_file(filename, outdir)
