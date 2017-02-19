#!/usr/bin/env python
import logging
import time

logging.basicConfig(level=logging.DEBUG)

from tlv import *

lg = logging


lg.basicConfig(level=lg.INFO)

FILE="tlv.dat"

with open(FILE, "a+b") as f:

    t = TLV([
        TLV(16),
        TLV(32),
        TLV(b"ab"),
        TLV({TLV(b"key"): TLV(b"value")}),
        TLV([
            TLV(2560),
            TLV(25600),
            TLV(256000),
            TLV(2560000),
            TLV(25600000)
            ]),
        TLV(0.00000001),
        ])
    # t = TLV("Q", 2, 1)
    print(t)

    # Reset file
    f.seek(0)
    f.truncate()

    # Set stream
    t.setSource(f)

    # Dump
    s = time.time()
    t.write(0)
    f.flush()
    e = time.time()
    print("WRITE TIME: %f ms" % ((e-s)*1000))

    s = time.time()
    t2 = TLV(fd=f)
    t2.read(0)
    e = time.time()
    print("READ TIME: %f ms" % ((e-s)*1000))
    print(t2)

    s = time.time()
    t3 = TLV(fd=f)
    size = t3.size(0)
    e = time.time()
    print("SIZE TIME: %f ms" % ((e-s)*1000))
    print(size)
