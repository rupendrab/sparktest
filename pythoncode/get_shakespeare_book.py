#!/usr/bin/env python

from bookdownload import *

if __name__ == '__main__':
    from sys import argv
    if len(argv) < 2:
        print("Usage: %s <URL> <File Name (Optional>" % argv[0])
        sys.exit(1)
    url = argv[1]
    fileName = None
    if len(argv) >= 3:
        fileName = argv[2]
    download_file(url, fileName)
