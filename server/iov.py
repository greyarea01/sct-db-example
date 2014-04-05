#!/usr/bin/env python

import time

def getIOV(iov):
    if iov=='now':
        iov_snapshot=int(time.time())*1000000000
    else:
        iov_snapshot=int(iov)

    return iov_snapshot
