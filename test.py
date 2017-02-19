#!/usr/bin/env python
import os
import sys
import glob
import random
import logging
import argparse
import unittest

logging.basicConfig(level=logging.DEBUG)

from tlvdb.tlv import *
from tlvdb.tlvdb import *

lg = logging

# restrict logging to low-level implementation
lg.getLogger("tlv").setLevel(lg.INFO)

ROOT = os.path.abspath(os.path.dirname(__file__))

def main():
    parser = argparse.ArgumentParser(description='Test arguments')
    parser.add_argument('-t', '--tests', metavar='N', type=str, nargs='*', help='tests to run')
    parser.add_argument('-v', '--verbosity', metavar='N', type=int, default=3, help='unittest versobsity level')
    parser.add_argument('-r', '--reset', type=bool, default=False, help='Cleanup all test data')
    popts = parser.parse_args()

    if popts.reset is True:
        files = glob.glob('%s/data/*.idx' % ROOT)
        files.extend(glob.glob('%s/data/*.dat' % ROOT))
        for f in files:
            os.remove(f)

    if not popts.tests:
        suite = unittest.TestLoader().discover(os.path.dirname(__file__)+'/tests')
        #print(suite._tests)

        # Print outline
        lg.info(' * Going for Interactive net tests = '+str(not tvars.NOINTERACTIVE))

        # Run
        rc = unittest.TextTestRunner(verbosity=popts.verbosity).run(suite)
    else:
        lg.info(' * Running specific tests')

        suite = unittest.TestSuite()

        # Load standard tests
        for t in popts.tests:
            lg.info('   - Adding tests.%s' % t)
            test = unittest.TestLoader().loadTestsFromName("tests.%s" % t)
            suite.addTest(test)

        # Run
        rc = unittest.TextTestRunner(verbosity=popts.verbosity).run(suite)

    return len(rc.failures)


if __name__ == '__main__':
    sys.exit(main())
