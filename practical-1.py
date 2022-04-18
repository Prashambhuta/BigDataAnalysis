#!usr/bin/env python3

from time import perf_counter, sleep
from threading import Thread
from concurrent.futures import thread
from tracemalloc import start
import requests
from sqlalchemy import true

def download(url):
    print(f"Downloading file {url}")
    r = requests.get(url, allow_redirects=True)
    filename = url.split("/")[-1]
    open('', 'wb').write(r.content)

starttime = perf_counter()
threads = []
files = ['http://www.africau.edu/images/default/sample.pdf',
         'http://www.africau.edu/images/default/sample.pdf',
         'http://www.africau.edu/images/default/sample.pdf',
         'http://www.africau.edu/images/default/sample.pdf'
]

for n in files:
    t = Thread(target=download, args=(n,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

end = perf_counter()
print(end - starttime)