
import os
import unittest
from libs import files
from libs.logger import DummyLogger

class TestLibsFiles(unittest.TestCase):
    def setUp(self):
        self.logger = DummyLogger()

        self.data_folder = os.path.dirname(os.path.abspath(__file__))

        self.paths = {
            'checksum_test.dat': os.path.join(self.data_folder, 'data', 'checksum_test.dat'),
            'gaps.txt': os.path.join(self.data_folder, 'data', 'Level2pass2_IC86.2015_data_Run00127338_Subrun00000386_gaps.txt'),
            'grl2016': os.path.join(self.data_folder, 'data', 'GRL_2016.txt'),
            'tmp': os.path.join(self.data_folder, 'data', 'tmp.dat')
        }

        self.file_checksum = files.File(self.paths['checksum_test.dat'], self.logger)
        self.file_gaps = files.GapsFile(self.paths['gaps.txt'], self.logger)

    def test_file_checksum(self):
        f = files.File(self.paths['checksum_test.dat'], self.logger)

        self.assertEqual(f.md5(), 'd41d8cd98f00b204e9800998ecf8427e')
        self.assertEqual(f.sha512(), 'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e')

    def test_file_size(self):
        f = files.File(self.paths['checksum_test.dat'], self.logger)
        self.assertEqual(f.size(), 0)

    def test_file_path(self):
        f = files.File(self.paths['checksum_test.dat'], self.logger)
        self.assertEqual(f.path, self.paths['checksum_test.dat'])

    def test_gapsfile_path(self):
        self.assertEqual(self.file_gaps.path, self.paths['gaps.txt'])

    def test_gapsfile_content(self):
        self.file_gaps.read()
        self.assertFalse(self.file_gaps.has_gaps())
        self.assertEqual(self.file_gaps.get_run_id(), 127338)
        self.assertEqual(self.file_gaps.get_sub_run_id(), 386)
        self.assertEqual(self.file_gaps.get_first_event()['event'], 83717938)
        self.assertEqual(self.file_gaps.get_last_event()['event'], 83934646)
        self.assertEqual(self.file_gaps.get_file_livetime(), 73.53)

    def test_grl(self):
        grl = files.GoodRunList(self.paths['grl2016'], self.logger)
        grl.load()

        first_run = min(grl.keys())
        last_run = max(grl.keys())

        self.assertEqual(len(grl), 1059)
        self.assertEqual(first_run, 127951)
        self.assertEqual(last_run, 129297)
        self.assertTrue(grl.has_run(128651))

        run_data = grl.get_run(128651)

        self.assertEqual(run_data['RunNum'], 128651)
        self.assertEqual(run_data['Good_it'], 1)
        self.assertEqual(run_data['Good_i3'], 1)
        self.assertEqual(run_data['LiveTime'], 28771)
        self.assertEqual(run_data['ActiveStrings'], 87)
        self.assertEqual(run_data['ActiveDoms'], 5631)
        self.assertEqual(run_data['ActiveInIce'], 5110)
        self.assertEqual(run_data['OutDir'], '/data/exp/IceCube/2016/filtered/level2/1027/Run00128651/')

        grl2 = files.GoodRunList(self.paths['tmp'], self.logger)
        grl2.add_run({'RunNum': 123, 'OutDir': '/data/user/testfile.dat'})

        self.assertRaises(Exception, grl2.add_run, {'RunNum': 123})

        self.assertRaises(Exception, grl2.write)

        grl2.mode = 'w'
        grl2.write()

        self.assertEqual(grl2.sha512(), 'a85b4f0ce5bfe676b4c14a7711daf0764087b76d5809218d6bae7696c3a2b221d5824bff16187d06f964a0c1dcf84e0391a7380b1e6fc44eb7e38a94526ff189')

        grl2.remove()
        self.assertFalse(grl2.exists())

