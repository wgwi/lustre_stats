import logging
import functools
from typing import List, Any, Tuple, Dict, TypeVar
import sys
import argparse
import pprint
import asyncio
import time
import re
from functools import partial
import psutil
from abc import ABC

CONST_HEALTH_FILE = '/sys/fs/lustre/health_check'

logging.basicConfig(
        filename='lustre_monitor.log',
        level=logging.DEBUG,
        format='%(levelname)s:%(asctime)s:%(message)s'
)

print(psutil.cpu_percent())
print(psutil.virtual_memory())

# MDS, OSS both support
def read_health()-> bool: 
    with open(CONST_HEALTH_FILE, 'r') as f:
        if f.read().startswith('healthy'):
            return True
        return False


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


def parse_T(lines :str):
    res = re.split(['\.|=|\n'], instre)
    ### parse multi-line "osd-ldiskfs.sxu-MDT0000.filesfree=177243814"
    return (res[1], int(res[-1]))


MDSds = [('mdt.*MDT*.num_exports', 'Number of connections', 'mdt.\w*.number_exports=\d*'action), 
         ### mdt.sxu-MDT0000.num_exports=73 #
         ('mdt.*.md_stats', 'MDS stats', action),
         ### file:///mds_stats.res
         ('mdt.*MDT*.exports.*@*.stats', 'MDS per-node stats', action),
         ### file:///mds_per_stats.res
         ('osd-*.*MDT*.filesfree', 'MDS free available', action),
         ### osd-ldiskfs.sxu-MDT0000.filesfree=177243814
         ('osd-*.*MDT*.filestotal', 'MDS total files number', action),
         ### osd-ldiskfs.sxu-MDT0000.filestotal=187112176
         ('osd-*.*MDT*.kbytesfree', 'MDS total disk free', action),
         ### osd-ldiskfs.sxu-MDT0000.kbytesfree=270335084
         ('osd-*.*MDT*.kbytestotal', 'MDS total disk used', action)
         ### osd-ldiskfs.sxu-MDT0000.kbytestotal=274279768
         ]

OSSds = [('obdfilter.*.stats', 'OSS stats', action),
         ### file://oss_stats.res
         ('obdfilter.*OST*.exports.*@*.stats', 'OSS per-node stats', action),
         ### file://oss_per_stats.res
         ('obdfilter.*OST*.kbytesfree', 'OSS total disk free', action),
         ### obdfilter.sxu-OST0003.kbytesfree=19882727936
         ('obdfilter.*OST*.kbytestotal', 'OSS total disk used', action),
         ### obdfilter.sxu-OST0003.kbytestotal=50314586112
         ]
