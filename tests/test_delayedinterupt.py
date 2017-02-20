import os
import signal
from tlvdb.util import DelayedInterrupt
from mock import Mock
import unittest


class TestDelayed(unittest.TestCase):
    def test_delayed_interrupt_with_one_signal(self):
        # check behavior without DelayedInterrupt
        a = Mock()
        b = Mock()
        c = Mock()
        try:
            a()
            os.kill(os.getpid(), signal.SIGINT)
            b()
        except KeyboardInterrupt:
            c()
        a.assert_called_with()
        b.assert_not_called()
        c.assert_called_with()

        # test behavior with DelayedInterrupt
        a = Mock()
        b = Mock()
        c = Mock()
        try:
            with DelayedInterrupt(signal.SIGINT):
                a()
                os.kill(os.getpid(), signal.SIGINT)
                b()
        except KeyboardInterrupt:
            c()
        a.assert_called_with()
        b.assert_called_with()
        c.assert_called_with()


    def test_delayed_interrupt_with_multiple_signals(self):
        a = Mock()
        b = Mock()
        c = Mock()
        try:
            with DelayedInterrupt([signal.SIGTERM, signal.SIGINT]):
                a()
                os.kill(os.getpid(), signal.SIGINT)
                os.kill(os.getpid(), signal.SIGTERM)
                b()
        except KeyboardInterrupt:
            c()
        a.assert_called_with()
        b.assert_called_with()
        c.assert_called_with()
