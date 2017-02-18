#!/usr/bin/env python
import logging
import random
logging.basicConfig(level=logging.DEBUG)

from tlv import *
from tlvdb import *

lg = logging

# restrict logging to low-level implementation
lg.getLogger("tlv").setLevel(lg.INFO)

IFILE = "./test.idx"

# ITEMS = 100*100
ITEMS = 100*10

ts = TlvStorage(IFILE)
idx = ts.index

def headerDump():
    print("%15s: %s" % ("Header Version", idx.header.version))
    print("%15s: %s" % ("Index Type", idx.header.type))
    print("%15s: %s" % ("Num Entries", idx.header.items))
    # print("%15s: %s" % ("Num Empty", idx.header.empty))
    print("%15s: %s" % ("Partitions", idx.header.partitions))

headerDump()
print("Creating")
ts.beginTransaction()

for i in range(0, ITEMS):
    # t = TLV([
    #         TLV({
    #             TLV(("key%d" % (idx.nextid)).encode("ascii")): TLV(("value%d" % (idx.nextid)).encode("ascii"))
    #         }),
    #         TLV(idx.nextid),
    #         TLV(1),
    #         TLV(b"static")
    #     ])
    t = TLV({
                TLV(("key%d" % (idx.nextid)).encode("ascii")): TLV(("value%d" % (idx.nextid)).encode("ascii"))
            })
    ts.create(t)

ts.endTransaction()

headerDump()
print("Getting %d" % (idx.nextid-1))
t = ts.read(idx.nextid-1)
print(t)


print("Deleting")
ts.beginTransaction()
# Delete the second half... from what we just added
for i in range(idx.nextid-1, int(idx.nextid-ITEMS/2-1), -1):
    t = ts.delete(i, TLV)
    # print(t)
ts.endTransaction()

headerDump()

if random.random() < 1.1:
    ts.vacuum()
    headerDump()
