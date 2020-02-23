import sys
import re
import os
import pprint
from enum import Enum

class MO(Enum):
    MDS = 1
    OSS = 2


def parse_Stats(lines: str, mo: MO):
    ##################################################################
    ### parse multi-parts mds_stats, detail in mds_stats.res
    ### param cares about: snapshot_time, disk_name, open, sync                          
    ##################################################################
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
            
if __name__ == '__main__':
    input = sys.argv[1]
    with open(input, 'r') as f:
        lines = f.read()
        pprint.pprint(parse_Stats(lines, MO.OSS))
