#!/usr/bin/env python
import os
import sys
import logging as lg

lg.basicConfig(level=lg.INFO)

from tlvdb.tlvstorage import TlvStorage

dbindexfile = sys.argv[1]
storage = TlvStorage(dbindexfile)
print(storage.index.header.getStrInfo())
print(storage.index.getStrInfo())
for i in range(storage.index.nextid-1, 0, -1):
    t = storage.read(i)
    print(" - %d: %s" % (i, t))
