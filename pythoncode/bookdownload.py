from bs4 import BeautifulSoup
import urllib
import sys

def get_filename(url):
    spl = url.split("/")
    if spl[-1] == 'full.html':
        return spl[-2] + ".txt"
    else:
       return spl[-1].replace(".html", ".txt")
    
def download_file(url, fileName = None):
    if not fileName:
        fileName = get_filename(url)
    r = urllib.urlopen(url)
    outfile = open(fileName, 'w')
    soup = BeautifulSoup(r, "html.parser")
    # for line in [x.get_text() for x in soup.find('h3').findAllNext()]:
    for line in [soup.get_text()]:
        outfile.write(line)
    outfile.close()
