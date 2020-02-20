#!/share/software/anaconda3/bin/python
import logging
import functools
from typing import List, Any, Tuple, Dict
import sys
import argparse
import pprint
import asyncio 
import time
from functools import partial
from prometheus_client import Gauge, CollectorRegistry, pushadd_to_gateway
import os
from os.path import join, dirname
from dotenv import load_dotenv

logging.basicConfig(
        filename='lsload.log',
        level=logging.DEBUG,
        format='%(levelname)s:%(asctime)s:%(message)s'
)

load_dotenv()

def proc_line(line: str):
    st_fn = lambda x: 0 if x == 'ok' else 1
    to_int_fn = lambda x: int(x[:-1])
    t = line.strip()
    if len(t) == 0:
        return None
    rows = t.split()
    if rows[1] == 'ok':
        return [rows[0], st_fn(rows[1]), to_int_fn(rows[5]), to_int_fn(rows[11])]
    else:
        return [rows[0], st_fn(rows[1])]

def flush_to_gateway(v:List[Any], target: str):
    reg = CollectorRegistry()
    st_metric = Gauge('status', 'Node status', ["host"], registry=reg)
    cpu_metric = Gauge('cpu', 'CPU using percent', ["host"], registry=reg)
    mem_metric = Gauge('mem', 'Mem using GB', ["host"], registry=reg)

    for i in v:
        st_metric.labels(host=i[0]).set(i[1])
        if i[1] == 0:
            cpu_metric.labels(host=i[0]).set(i[2])
            mem_metric.labels(host=i[0]).set(i[3])
        try:
            pushadd_to_gateway(target, job='hpcMonitor', registry=reg)
        except Exception as e:
            logging.ERR("Failt to upload:" + str(e))
    

def proc_stdout(line: str, target: str):
    rows = line.split('\n') #bypass head
    vs = [x for x in map(proc_line, rows[1:]) if x != None]
    if len(vs):
        flush_to_gateway(vs, target)


async def run_cmd(cmd: str, callback, sec_await) -> (str, int):
    proc = await asyncio.create_subprocess_exec(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        logging.ERR(f'[{cmd!r} exited with {proc.returncode}]')
        logging.ERR(stderr.decode())
    else:
        if stdout:
            callback(stdout.decode())
            await asyncio.sleep(sec_await)

if __name__ == '__main__':
    logging.info('lsload processing begin...')
    #parser = argparse.ArgumentParser()
    #parser.add_argument("t", help="pushgateway url")
    #args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        while True:
            loop.run_until_complete(run_cmd('lsload', partial(proc_stdout, target=os.getenv('PROME_ADDR')), 10))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    logging.info('lsload processing end.')
