import unittest

from string_test import TestString
from list_test import TestList

def create_test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestString))
    suite.addTest(unittest.makeSuite(TestList))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    test_suite = create_test_suite()
    runner.run(test_suite)
