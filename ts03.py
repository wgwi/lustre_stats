import sys
import re
import os
import pprint
from enum import Enum

class MO(Enum):
    MDS = 1
    OSS = 2


def parse_Per_Stats(lines: str, mo: MO):
    if mo == MO.MDS:
        CONST_PARAM = ['snap', 'open']  ## not same like one_line_stats
        CONST_REGEX = 'mdt.([\w\-]+).exports.([\d\.]+)@tcp.stats='
    else:
        ### why OSS use TCP stats not IB stats
        CONST_PARAM = ['snap', 'read', 'writ']
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
            if tt_list[0][:4] in CONST_PARAM:
                be_back[disk_id][client_ip][tt_list[0]] = tt_list[1]
    return be_back

if __name__ == '__main__':
    input = sys.argv[1]
    with open(input, 'r') as f:
        lines = f.read()
        pprint.pprint(parse_Per_Stats(lines, MO.MDS))
