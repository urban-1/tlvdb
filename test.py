#!/usr/bin/env python
import os
import sys
import glob
import random
import logging
import argparse
import unittest

logging.basicConfig(level=logging.DEBUG)

from tests import *
from tlvdb import *


lg = logging

# restrict logging to low-level implementation
lg.getLogger("tlv").setLevel(lg.WARNING)

ROOT = os.path.abspath(os.path.dirname(__file__))

def main():
    parser = argparse.ArgumentParser(description='Test arguments')
    parser.add_argument('-t', '--tests', metavar='N', type=str, nargs='*', help='tests to run')
    parser.add_argument('-v', '--verbosity', metavar='N', type=int, default=3, help='unittest versobsity level')
    parser.add_argument('-r', '--reset', default=False, action='store_true', help='Cleanup all test data')
    popts = parser.parse_args()

    if popts.reset is True:
        lg.info(' * Cleaning up old tests')
        files = glob.glob('%s/data/*.idx' % ROOT)
        files.extend(glob.glob('%s/data/*.dat' % ROOT))
        for f in files:
            lg.info('   - rm %s' % f)
            os.remove(f)

    # Set level
    if popts.verbosity == 3:
        lg.getLogger().setLevel(level=lg.DEBUG)
    elif popts.verbosity == 2:
        lg.getLogger().setLevel(level=lg.INFO)
    elif popts.verbosity == 1:
        lg.getLogger().setLevel(level=lg.WARN)
    elif popts.verbosity == 0:
        lg.getLogger().setLevel(level=lg.ERROR)

    if not popts.tests:
        suite = unittest.TestLoader().discover(os.path.dirname(__file__)+'/tests')

        # Print outline
        lg.info(' * Running all tests')

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
