
import os
import unittest
from libs import stringmanipulation
from libs.logger import DummyLogger

class TestLibsStringmanipulation(unittest.TestCase):
    def setUp(self):
        self.logger = DummyLogger()

        self.data_folder = os.path.dirname(os.path.abspath(__file__))

    def test_replace_var(self):
        test_str = 'Hi {user}, this is a test string with {len:d} chars. This var {v!r:>30} will be replaced with *. This one wont: {t!a:0>4}'
        test_str_result = 'Hi {user}, this is a test string with {len:d} chars. This var * will be replaced with *. This one wont: {t!a:0>4}'

        self.assertEqual(stringmanipulation.replace_var(test_str, 'v', '*'), test_str_result)

