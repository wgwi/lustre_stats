import logging
import functools
from typing import List, Any, Tuple, Dict, TypeVar
import sys
import argparse
import pprint
import asyncio
import time
import re
import itertool
from functools import partial
import psutil
from enum import Enum
from abc import ABC


logging.basicConfig(
        filename='lustre_monitor.log',
        level=logging.DEBUG,
        format='%(levelname)s:%(asctime)s:%(message)s'
)

print(psutil.cpu_percent())
print(psutil.virtual_memory())


async def run_cmd(cmd: str, callback) -> (str, int):
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

##############################################
# MDS, OSS both support
# Healthy and Version
##############################################
CONST_HEALTH_FILE = '/sys/fs/lustre/health_check'
CONST_LUSTRE_VERSION = '/sys/fs/lustre/version'

def read_health()-> bool: 
    with open(CONST_HEALTH_FILE, 'r') as f:
        if f.read().startswith('healthy'):
            return True
        return False

def read_version()-> str: 
    with open(CONST_LUSTRE_VERSION, 'r') as f:
        return f.read()


##################################################################
### parse multi-line "osd-ldiskfs.sxu-MDT0000.filesfree=177243814"
###                               ^^^^^^^^^^^
###---------------------------------------------------------------
### MDS num_exports, freefiles, totalfiles, kbytesfree, kbytestotal
### OSS kbytesfree, kbytestotal
##################################################################
def parse_KV(lines :str):
    rows = lines.split('\n')
    vs = [(x[1], x[-1]) for x in map(re.split('\.|='), rows)]
    return vs


class MO(Enum):
    MDS = 1
    OSS = 2


##################################################################
### parse multi-parts mds_stats, detail in mds_stats.res
### param cares about: snapshot_time, disk_name, open, sync                          
##################################################################
def parse_Stats(lines: str, mo: MO):
    ts = []  
    vs = {}  ## used to store {dist_name, snapshot_time, open_number, sync_number}
    if mo == MO.MDS:
        CONST_PARAM = ['snap', 'open', 'sync']
    else:
        CONST_PARAM = ['snap', 'read', 'writ', 'sync', 'conn', 'reco', 'disc']

    rows = lines.split('\n')
    for row in rows:
        if len(row) == 0:
            continue
        if row[-1] == '=':  ## a new disk record, store formal record
            ts.append(vs)
            vs['disk'] = row.split('.')[1]
        else:
            tt_list = row.split()
            if tt_list[0][:4] in CONST_PARAM:
                vs[tt_list[0]] = tt_list[1]
    return ts


def parse_Per_Stats(lines: str, mo: MO):
    ### mdt.sxu-MDT0000.exports.192.168.1.102@tcp.stats=
    ###     ^^^^^^^^^^^         ^^^^^^^^^^^^^
    ### obdfilter.sxu-OST0003.exports.192.168.1.7@tcp.stats=
    ###           ^^^^^^^^^^^         ^^^^^^^^^^^
    ### 
    if mo == MO.MDS:
        CONST_REGEX = 'mdt.([\w\-]+).exports.([\d\.]+)@tcp.stats='
    else:
        ### why OSS use TCP stats not IB stats
        CONST_REGEX = 'obdfilter.([\w\-]+).exports.([\d\.]+)@tcp.stats='


    rows = lines.split('\n')
    if len(rows) == 0:
        return []

    io_flag = True
    be_back = {}
    (disk_id, client_ip) = (None, None)
    for row in rows:
        if len(row) == 0:
            continue
        if row[-1] == '=':  ## lo.stats or tcp.stats
            res = re.findall(CONST_REGEX, row)
            if len(res):  ## tcp.stats
                io_flag = False
                (disk_id, client_ip) = res[0]
                if disk_id not in be_back:
                    be_back[disk_id] = {client_ip: {}}
                else:
                    be_back[disk_id][client_ip] = {}
            else: ## lo.stats
                io_flag = True
        elif io_flag == True:
            continue
        else:
            tt_list = row.split()
            if tt_list[0][:4] in ['snap', 'open']:
                be_back[disk_id][client_ip][tt_list[0]] = tt_list[1]
            elif tt_list[0][:4] in ['read', 'writ']:
                be_back[disk_id][client_ip][tt_list[0]] = tt_list[6]

    return be_back


MDSds = [('mdt.*MDT*.num_exports', 'Number of connections', parse_KV), 
         ### mdt.sxu-MDT0000.num_exports=73 
         ('mdt.*.md_stats', 'MDS stats', parse_Stats),
         ### file:///mds_stats.res
         ('mdt.*MDT*.exports.*@*.stats', 'MDS per-node stats', parse_Per_Stats),
         ### file:///mds_per_stats.res
         ('osd-*.*MDT*.filesfree', 'MDS free available', parse_KV),
         ### osd-ldiskfs.sxu-MDT0000.filesfree=177243814
         ('osd-*.*MDT*.filestotal', 'MDS total files number', parse_KV),
         ### osd-ldiskfs.sxu-MDT0000.filestotal=187112176
         ('osd-*.*MDT*.kbytesfree', 'MDS total disk free', parse_KV),
         ### osd-ldiskfs.sxu-MDT0000.kbytesfree=270335084
         ('osd-*.*MDT*.kbytestotal', 'MDS total disk used', parse_KV)
         ### osd-ldiskfs.sxu-MDT0000.kbytestotal=274279768
         ]

OSSds = [('obdfilter.*.stats', 'OSS stats', parse_Stats),
         ### file://oss_stats.res
         ('obdfilter.*OST*.exports.*@*.stats', 'OSS per-node stats', parse_Per_Stats),
         ### file://oss_per_stats.res
         ('obdfilter.*OST*.kbytesfree', 'OSS total disk free', parse_KV),
         ### obdfilter.sxu-OST0003.kbytesfree=19882727936
         ('obdfilter.*OST*.kbytestotal', 'OSS total disk used', parse_KV),
         ### obdfilter.sxu-OST0003.kbytestotal=50314586112
         ]

async def Loop(mo: MO, timeToSleep: int):
    cmd_build = lambda x: f'lctl getparam {x}'
    call_back_fn = lambda x: partial(x, mo = mo)

    while True:
        if mo == MO.MDS:
            tasks = [async.create_task(run_cmd(com_build(x[0]), call_back_fun(x[2]))) for x in MDSds]
        else:
            tasks = [async.create_task(run_cmd(com_build(x[0]), call_back_fun(x[2]))) for x in OSSds]

        await asyncio.gather(*tasks)
        await asyncio.sleep(timeToSleep)


# here is the plan, the processing looping every 10second, all the job
# run as task, the major receive and processing all the results,
# then upload to the pushgateway, or upload to the prometheus?
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("t", help="pushgateway url")
    args = parser.parse_args()

    asyncio.run()

