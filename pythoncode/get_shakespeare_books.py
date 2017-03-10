#!/usr/bin/env python

from bookdownload import *

def download_files(linksFile, outDir):
    for line in open(linksFile, 'r'):
        url = line[:-1]
        if url:
            fileName = get_filename(url)
            print("Downloading book %s" % (fileName))
            download_file(url, outDir + "/" + fileName)

if __name__ == '__main__':
    from sys import argv
    if len(argv) != 3:
        print("Usage: %s <Links File> <Download Directory>" % argv[0])
        sys.exit(1)
    linksFile = argv[1]
    outDir = argv[2]
    download_files(linksFile, outDir)
