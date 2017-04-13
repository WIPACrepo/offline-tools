
import os
import path
import json

import utils

from I3Tray import *

class File(object):
    def __init__(self, path, logger):
        self.path = path
        self.logger = logger

    def size(self):
        return os.path.getsize(self.path)

    def exists(self):
        return os.path.isfile(self.path)

    def remove(self):
        os.remove(self.path)

    def checksum(self, ctype, buffersize = 16384):
        """Return checksum of type `type` digest of file"""

        self.logger.debug("Try to open file for {ctype} checksum sum: {path}".format(ctype = ctype, path = self.path))

        with open(self.path) as f:
            import hashlib
            digest = None

            if ctype.lower() == 'md5':
                digest = hashlib.md5()
            elif ctype.lower() == 'sha512':
                digest = hashlib.sha512()
            else:
                raise Exception('Unsupported checksum type: {0}'.format(ctype))

            self.logger.debug("Read file")

            buffer = f.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = f.read(buffersize)

            self.logger.debug("Close file")
        
        return digest.hexdigest()

    def md5(self, buffersize = 16384):
        return self.checksum('md5', buffersize)

    def sha512(self, buffersize = 16384):
        return self.checksum('sha512', buffersize)

    def get_name(self):
        return os.path.basename(self.path)

    def get_dirname(self):
        return os.path.dirname(self.path)

    @classmethod
    def get_md5(cls, path, logger):
        f = cls(path, logger)
        return f.md5()

    @classmethod
    def get_sha512(cls, path, logger):
        f = cls(path, logger)
        return f.sha512()

    @classmethod
    def get_checksum(cls, path, ctype, logger):
        f = cls(path, logger)
        return f.checksum(ctype)

class GoodRunList(File):
    def __init__(self, path, logger, columns = ['RunNum', 'Good_i3', 'Good_it', 'LiveTime', 'ActiveStrings', 'ActiveDoms', 'ActiveInIce', 'OutDir', 'Comment(s)'], run_id_column = 0, empty_cell_value = 'N/A', mode = 'r', num_decimals = 2):
        super(GoodRunList, self).__init__(path, logger)

        self.columns = columns
        self._run_id_column = run_id_column
        self._data = {}
        self._empty_cell_value = empty_cell_value
        self.mode = mode
        self.num_decimals = num_decimals

    def __iter__(self):
        self._iter = iter(self._data)
        return self

    def __next__(self):
        return self._data[next(self._iter)]

    def next(self):
        return self.__next__()

    def iterkeys(self):
        return self._data.iterkeys()

    def itervalues(self):
        return self._data.itervalues()

    def iteritems(self):
        return self._data.iteritems()

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def __len__(self):
        return len(self._data)

    def load(self):
        self._data = {}

        with open(self.path, 'r') as f:
            headers = True

            for line in f:
                # Ignore comments (= lines that start with #)
                if line.strip().startswith('#'):
                    continue

                # Ignore old-style GRL headers but only if no data has been read yet
                if headers and (line.strip().startswith('RunNum') or line.strip() == '(1=good 0=bad)'):
                    continue

                # Ignore empty lines
                if not len(line.strip()):
                    continue

                headers = False

                # Fill data
                columns = [c.strip() for c in line.split(None, len(self.columns) - 1)]

                data = {}
                for column, value in enumerate(columns):
                    data[self.columns[column]] = value.strip()

                self.add_run(data)

    def add_run(self, data):
        run_id = int(data[self.columns[self._run_id_column]])

        if self.has_run(run_id):
            raise Exception("Run {run_id} has already been added to the GRL.".format(run_id = run_id))

        def convert_to_number(num, num_decimals):
            if isinstance(num, int):
                return num
            elif isinstance(num, float):
                return round(num, num_decimals)

            try:
                return int(num)
            except ValueError:
                try:
                    return round(float(num), num_decimals)
                except ValueError:
                    return str(num)

        self._data[run_id] = {k: convert_to_number(v, self.num_decimals) for k, v in data.items()}

    def get_run(self, run_id):
        return self._data[run_id]

    def has_run(self, run_id):
        return run_id in self._data

    def get_run_ids(self):
        return sorted(self._data.keys())

    def write(self):
        if self.mode != 'w':
            raise Exception('You are not allowed to write this file. In order to write this file set the mode to \'w\'.')

        # Find max with for each column
        max_width = {c: max(len(str(c)), len(str(self._empty_cell_value))) for c in self.columns}
        for run_id, value in self._data.items():
            for column, value in value.items():
                if len(str(value)) > max_width[column]:
                    max_width[column] = len(str(value))

        def create_line(columns, widths, data, default_value):
            line = ''

            for column in columns:
                value = default_value

                if column in data:
                    value = data[column]

                line += str(value).ljust(widths[column] + 1)

            return line

        # Check if destination folder exists
        if not os.path.isdir(self.get_dirname()):
            self.logger.warning('Folder {0} does not exist. Create folder'.format(self.get_dirname()))
            os.mkdir(self.get_dirname())

        with open(self.path, 'w') as f:
            # Write header
            f.write(create_line(self.columns, max_width, {c: c for c in self.columns}, self._empty_cell_value) + '\n')

            sorted_runs = sorted(self._data.keys())
            for run_id in sorted_runs:
                f.write(create_line(self.columns, max_width, self._data[run_id], self._empty_cell_value) + '\n')

class GapsFile(File):
    def __init__(self, path, logger):
        super(GapsFile, self).__init__(path, logger)
        self._values = None

    def read(self, force = False):
        if self._values is not None and not force:
            return

        self._values = {}
    
        with open(self.path, 'r') as file:
            for line in file:
                pair = line.split(':')
    
                if len(pair) != 2:
                    raise
    
                key = pair[0].strip()
                value = pair[1].strip()
    
                if pair[0] == 'First Event of File':
                    key = 'first event'
                elif pair[0] == 'Last Event of File':
                    key = 'last event'
    
                if pair[0] == 'First Event of File' or pair[0] == 'Last Event of File':
                    tmp = value.split(' ')
                    value = {'event': int(tmp[0].strip()),
                            'year': int(tmp[1].strip()),
                            'frac': int(tmp[2].strip())}
    
                if pair[0] == 'Gap Detected':
                    tmp = value.split(' ')
                    key = 'gap'
                    value = {'dt': float(tmp[0].strip()),
                            'prev_event_id': int(tmp[1].strip()),
                            'prev_event_frac': int(tmp[2].strip()),
                            'curr_event_id': int(tmp[3].strip()),
                            'curr_event_frac': int(tmp[4].strip())}

                    # A file can have several gaps
                    if key not in self._values:
                        self._values[key] = []
    
                if key == 'gap':
                    self._values[key].append(value)
                else:
                    self._values[key] = value
    
        self._values['subrun'] = path.get_sub_run_id_from_path(self.path, 'Level2', self.logger)

    def has_gaps(self):
        return 'gap' in self._values.keys()

    def get_gaps(self):
        if self.has_gap():
            return self._values['gap']
        else:
            return None

    def get_sub_run_id(self):
        return self._values['subrun']

    def get_run_id(self):
        return int(self._values['Run'])

    def get_first_event(self):
        return self._values['first event']

    def get_last_event(self):
        return self._values['last event']

    def get_file_livetime(self):
        return float(self._values['File Livetime'])

    def create_from_data(self, f, overwrite = False):
        """
        Reads the L2 file `f` and creates the gaps file. The filename has been specified with `GapsFile.path`. After
        the gaps file has been created, the data can be accessed through this class as well. `GapsFile.read()` is called automatically.

        If `GapsFile.path` is `None`, an Exception will be thrown. If `overwrite` is `False` and `GapsFile.path`
        exists, also an Exception is thrown.

        Args:
            f (str): Path to L2 data file
            overwrite (boolean): If the gaps file alreayd exists, it wil be overwritten if this option has been set to `True`
        """

        if self.path is None:
            raise Exception('No gaps file name has been specified')

        if os.path.isfile(self.path) and not overwrite:
            raise Exception('Gaps file already exist')

        from icecube import dataio, icetray, dataclasses
        from iceube.filterscripts.offlineL2 import SpecialWriter

        tray = I3Tray()
        tray.Add("I3Reader","readL2File", filename = f)
        tray.Add(SpecialWriter.GapsWriter, Filename = self.path, MinGapTime = 1)
        tray.AddModule("TrashCan","trash")
        tray.Execute()
        tray.Finish()

        # Gaps file has been created. Let's parse the gaps file to have easily access to the values
        self.read()

class ChecksumCache(utils.ChecksumCache, File):
    def __init__(self, logger, ctypes = ['md5', 'sha512']):
        from libs.config import get_config

        File.__init__(self, get_config(logger).get('CacheCheckSums', 'CacheFile'), logger)
        utils.ChecksumCache.__init__(self, logger, ctypes)

        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                self._data = json.load(f)

    def write(self):
        """
        Stores the data that has been added via the `set` methods in file or DB.
        """

        if self._data is None:
            self.logger.error('No data to write')
        else:
            with open(self.path, 'w') as f:
                json.dump(self._data, f)

class MetaXMLFile(File):
    def __init__(self, dest_folder, run, level, dataset_id, logger):
        super(MetaXMLFile, self).__init__(None, logger)

        if level not in ['L2', 'L3']:
            raise Exception('Level must be L2 or L3')

        from libs.config import get_config
        self.config = get_config(logger)

        if level == 'L2':
            self.path = os.path.join(dest_folder, self.config.get('Level2', 'MetaFileName'))
        elif level == 'L3':
            self.path = os.path.join(dest_folder, self.config.get('Level3', 'MetaFileName'))

        self.run = run
        self.level = level
        self.dataset_id = dataset_id

    def add_main_processing_info(self):
        """
        Writes a meta XML file for a specific run.

        Utilizes a lot of information from the config/offline_processing.cfg, which also specifies
        which template file should be used.

        Args:
            dataset_id (int): The dataset id if the run
        """

        import datetime

        # Get all information that is required
        now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        season = self.run.get_season()
        ts_first_name = self.config.get('PERSONNEL', 'FirstName')
        ts_last_name = self.config.get('PERSONNEL', 'LastName')
        ts_email = self.config.get('PERSONNEL', 'eMail')
        dif_creation_date = self.run.get_start_time().date_time.strftime('%Y-%m-%d')
        start_date_time = self.run.get_start_time().date_time.strftime('%Y-%m-%dT%H:%M:%S')
        end_date_time = self.run.get_stop_time().date_time.strftime('%Y-%m-%dT%H:%M:%S')
        subcategory = '' # assigned later
        subcategory_capitalized = '' # assigned later
        icerec_version = '' # assigned later
        file_name = '' # assigned later
        template_file = self.config.get('DEFAULT', 'MetaFileTemplateMainProcessing')
        working_group = None # Will be assigned automatically if it is a L3 dataset

        template = ''
        with open(template_file, 'r') as file:
            template = file.read()

        if self.level == 'L2':
            subcategory = 'level2'
            icerec_version = os.path.basename(self.config.get('Level2', 'I3_SRC'))
        elif self.level == 'L3':
            subcategory = 'level3'
            l3_datasets = self.config.get_var_dict('Level3', 'I3_SRC_', keytype = int)
            if self.dataset_id not in l3_datasets.keys():
                logger.critical("Dataset {0} is not configured in config file.".format(self.dataset_id))
                raise Exception("Dataset {0} is not configured in config file.".format(self.dataset_id))

            icerec_version = os.path.basename(l3_datasets[self.dataset_id])

        subcategory_capitalized = subcategory.title()

        if self.level == 'L3':
            working_groups = self.config.get_var_dict('Level3', 'WG', keytype = int)

            if working_groups[self.dataset_id] is None:
                logger.critical("Working group name is not defined for dataset {0}. Check config file.".format(self.dataset_id))
                raise Exception("Working group name is not defined for dataset {0}. Check config file.".format(self.dataset_id))

            subcategory_capitalized = "{0} ({1})".format(subcategory_capitalized, working_groups[self.dataset_id])

        if not os.path.isdir(self.get_dirname()):
            logger.critical("Folder '{0}' does not exist".format(dest_folder))
            raise Exception("Folder '{0}' does not exist".format(dest_folder))

        # Fill the template
        meta_file_content = template.format(
            season = season,
            subcategory_capitalized =subcategory_capitalized,
            run_id = self.run.run_id,
            ts_first_name = ts_first_name,
            ts_last_name = ts_last_name,
            ts_email = ts_email,
            dif_creation_date = dif_creation_date,
            start_date_time = start_date_time,
            end_date_time = end_date_time,
            subcategory = subcategory,
            icerec_version = icerec_version,
            now = now)

        with open(self.path, 'w') as f:
            self.logger.debug("Write meta file: {0}".format(self.path))
            f.write(meta_file_content)

    def add_post_processing_info(self, script_file):
        """
        Adds the post processing info to the xml meta file.

        Args:
            script_file (str): Path to the post processing script. Needs to sit in the root folder of the submission scripts.
        """

        import xml.etree.ElementTree as ET
        import xml.dom.minidom as minidom
        from libs.svn import SVN
        from libs.path import get_rootdir
        import datetime

        svn = SVN(get_rootdir(), self.logger)

        # Since it is the post processing, there should already be a meta file
        # If there is no meta file, display a warning.

        if not self.exists():
            self.logger.warning("Meta file '{0}' does not exist. That means that no meta information are avilable from main processing, and we can not proceed adding information.".format(self.path))
            return

        # Adding post processing information
        xml_tree = ET.parse(self.path)
        xml_root = xml_tree.getroot()
        xml_post_processing = ET.Element('Project')

        xml_name = ET.Element('Name')
        xml_version = ET.Element('Version')
        xml_date_time = ET.Element('DateTime')

        xml_name.text = os.path.join(svn.get('URL'), os.path.basename(script_file))
        xml_version.text = "Revision %s" % svn.get('Revision')
        xml_date_time.text = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        # Finding Plus section
        xml_plus = xml_root.find('Plus')
        if xml_plus is None:
            self.logger.critical("Cannot find 'Plus' elemnt in meta xml file {0}".format(path))
            exit(1)

        # The actual adding
        xml_post_processing.append(xml_name)
        xml_post_processing.append(xml_version)
        xml_post_processing.append(xml_date_time)

        xml_plus.append(xml_post_processing)

        # Writing file
        with open(self.path, 'w') as f:
            formatted_xml = minidom.parseString(ET.tostring(xml_root)).toprettyxml(indent = '    ')

            # It contains empty lines. Remove them
            formatted_xml = os.linesep.join([s for s in formatted_xml.splitlines() if s.strip()])

            self.logger.debug("Write meta file: {0}".format(path))
            f.write(formatted_xml)

def create_good_run_list(dataset_id, db, logger, dryrun):
    """
    Creates the GRLs: Versioned and unversioned. The versioned one uses the run folders with production version.

    Args:
        dataset_id (int): The dataset id that should be used for creating the dataset.
        db (DatabaseConnection): The database that contains the run information for L2 and L3
    """

    from config import get_config
    import datetime
    import copy
    from runs import Run
    from glob import glob
    from stringmanipulation import replace_var

    config = get_config(logger)
    dataset_info = config.get_dataset_info(dataset_id)

    # Find latest versions of GRLs:
    format_grl_path = lambda p: replace_var(p, 'now', '*').format(
        season = dataset_info['season'],
        year = dataset_info['season'],
        production_version = '*',
        snapshot_id = '*'
    )

    latest_grl_glob_str = format_grl_path(config.get('Level2', 'GRLFileName'))
    logger.debug('latest_grl_glob_str = {0}'.format(latest_grl_glob_str))
    latest_grl = glob(latest_grl_glob_str)

    latest_grl_versioned_glob_str = format_grl_path(config.get('Level2', 'GRLFileVersionedName'))
    logger.debug('latest_grl_versioned_glob_str = {0}'.format(latest_grl_versioned_glob_str))
    latest_grl_versioned = glob(latest_grl_versioned_glob_str)

    latest_grl.sort(key = lambda x: os.path.getmtime(x))
    latest_grl_versioned.sort(key = lambda x: os.path.getmtime(x))

    if len(latest_grl):
        latest_grl = File(latest_grl[-1], logger)
    else:
        latest_grl = None

    logger.debug('Latest GRL: {0}'.format(latest_grl))

    if len(latest_grl_versioned):
        latest_grl_versioned = File(latest_grl_versioned[-1], logger)
    else:
        latest_grl_versioned = None

    # It IS IMPORTANT that the result is sorted descendingwise for the production_version!
    runs = db.fetchall("""
        SELECT * FROM i3filter.runs r 
        JOIN i3filter.post_processing p
            ON r.run_id = p.run_id
        WHERE p.dataset_id = {dataset_id}
            AND p.validated
        ORDER BY r.run_id ASC, r.production_version DESC
    """.format(dataset_id = dataset_id))

    latest_production_version = max([d['production_version'] for d in runs] or [0])
    latest_snapshot_id = max([d['snapshot_id'] for d in runs], [0])

    # New GRLs
    format_grl_path = lambda p: p.format(
        season = dataset_info['season'],
        year = dataset_info['season'],
        production_version = latest_production_version,
        snapshot_id = latest_snapshot_id,
        now = datetime.datetime.now()
    )

    grl = GoodRunList(format_grl_path(config.get('Level2', 'GRLFileName').replace('%%', '%')), logger, mode = 'w')
    grl_versioned = GoodRunList(format_grl_path(config.get('Level2', 'GRLFileVersionedName').replace('%%', '%')), logger, mode = 'w')

    logger.debug("New file: {0}".format(grl.path))
    logger.debug("New file: {0}".format(grl_versioned.path))

    if dryrun:
        grl.path = os.path.join(path.get_tmpdir(), grl.get_name())
        grl_versioned.path = os.path.join(path.get_tmpdir(), grl_versioned.get_name())

        logger.warning('Dryrun: the GRLs will be written to {0} and {1}'.format(grl.path, grl_versioned.path))

    if grl.exists() or grl_versioned.exists():
        raise Exception("One good run list already exists.")

    for run in runs:
        # Check if run is alreayd in GRL. `runs` is sorted descending by production version. That means
        # that the latest production version of a given run comes first. Therefore, if a run has two
        # production_versions, the first one is the newest and only this should be added
        if grl.has_run(run['run_id']):
            continue

        format_run_path = lambda p: p.format(
            run_id = run['run_id'],
            season = dataset_info['season'],
            year = run['tstart'].year,
            month = run['tstart'].month,
            day = run['tstart'].day,
            production_version = run['production_version'],
            snapshot_id = run['snapshot_id'],
            now = datetime.datetime.now()
        )

        comment = ''

        if config.is_test_run(int(run['run_id'])):
            comment = "IC86_{0} 24hr test run".format(dataset_info['season'])

        run_obj = Run(run['run_id'], logger)

        grl_row = {
            'RunNum': run['run_id'],
            'Good_i3': run['good_i3'],
            'Good_it': run['good_it'],
            'LiveTime': run_obj.get_livetime(),
            'Comment(s)': comment
        }

        if run['active_strings'] is not None:
            grl_row['ActiveStrings'] = run['active_strings']

        if run['active_doms'] is not None:
            grl_row['ActiveDoms'] = run['active_doms']

        if run['active_in_ice_doms'] is not None:
            grl_row['ActiveInIce'] = run['active_in_ice_doms']

        # Not versioned version:
        grl_row['OutDir'] = format_run_path(config.get('Level2', 'RunLinkName'))
        grl.add_run(copy.deepcopy(grl_row))

        # Versioned version:
        grl_row['OutDir'] = format_run_path(config.get('Level2', 'RunFolder'))
        grl_versioned.add_run(grl_row)

    # Write files
    grl.write()
    grl_versioned.write()

    # Now check if latest and new files are actually differently. If not, delete the new one
    new_grl = True
    if latest_grl is not None:
        if grl.md5() == latest_grl.md5():
            grl.remove()
            new_grl = False
            logger.warning('New GRL has no updates. Delete new file.')

    new_grl_versioned = True
    if latest_grl_versioned is not None:
        if grl_versioned.md5() == latest_grl_versioned.md5():
            grl_versioned.remove()
            new_grl_versioned = False
            logger.warning('New versioned GRL has no updates. Delete new file.')

    # Create symlinks
    if new_grl:
        # Remove existing symlink
        symlink = File(format_grl_path(config.get('Level2', 'GRLLinkName')), logger)
        if symlink.exists():
            symlink.remove()

        path.make_relative_symlink(grl.path, symlink.path, dryrun, logger)

    if new_grl_versioned:
        # Remove existing symlink
        symlink = File(format_grl_path(config.get('Level2', 'GRLLinkVersionedName')), logger)
        if symlink.exists():
            symlink.remove()

        path.make_relative_symlink(grl_versioned.path, symlink.path, dryrun, logger)

def clean_datawarehouse(run, logger, dryrun):
    """
    Cleans the run folder (Level2) of the given run. Removes ALL files and ALL folders
    that are in the run folder.

    Args:
        run (runs.Run): The run
        logger (Logger): The logger
        dryrun (boolean): If `True`, this function does NOT delete files.
    """

    from config import get_config
    from glob import glob
    import shutil

    run_folder = run.format(get_config(logger).get('Level2', 'RunFolder'))

    files = glob(os.path.join(path, '*'))

    logger.debug('Found {0} files/folders in {1}'.format(len(files), run_folder))

    for f in files:
        logger.debug('Delete {0}'.format(f))

        if not dryrun:
            if os.path.isdir(f):
                shutil.rmtree(f)
            elif os.path.isfile(f):
                os.remove(f)
            else:
                raise Exception('{0} is not a file and not a folder'.format(f))

def tar_files(files, tar_file, logger, dryrun, mode = 'w'):
    """
    Tars all files into tar_file. If dryrun is `True`, the file will be created in get_tmpdir().

    Args:
        mode (str): Default is `w`. Same values as for tarfile.open() mode are allowed.
    """

    import tarfile
    import sys

    logger.info('Tar {0} files into {1}'.format(len(files), tar_file))

    if dryrun:
        tar_file = os.path.join(path.get_tmpdir(), os.path.basename(tar_file))
        logger.warning('Since it is a dryrun, the tar file will be {0}'.format(tar_file))

    if not len(files):
        logger.warning('No files to be tar-ed')
        return

    with tarfile.open(tar_file, mode) as tar:
        for f in files:
            file_name = None

            try:
                # In case it is a type of File
                tar.add(f.path, arcname = f.get_name())
                file_name = f.path
            except AttributeError:
                tar.add(f, arcname = os.path.basename(f))
                file_name = f

            logger.debug("Adding {0} to tar file".format(file_name))

def tar_log_files(run, logger, dryrun):
    from config import get_config
    from glob import glob

    config = get_config(logger)
    tar_name = run.format(config.get('Level2', 'TarLogFileName'))
    glob_str = config.get_var_list('Level2', 'LogFileGlob')

    globs = [run.format(gs) for gs in glob_str]

    files = []
    for g in globs:
        logger.debug('Find files: {0}'.format(g))
        files.extend(glob(g))

    logger.info('Found {0} log files that will be tar-ed'.format(len(files)))

    tar_files(files, tar_name, logger, dryrun, mode = 'w:bz2')

    logger.info('Delete the log files')
    for f in files:
        logger.debug('Delete {0}'.format(f))
        if not dryrun:
            os.remove(f)

def tar_gaps_files(iceprod, dataset_id, run, logger, dryrun):
    """
    Tar the gaps files together and update the urlpath table with 
    the newly created file

    Args:
        run (runs.Run): The run
        iceprod (iceprodinterface.IceProdInterface): Iceprod API
        dataset_id (int): The dataset id
        logger (Logger): The logger
        dryrun (boolea): Dryrun?
    """

    from config import get_config

    gaps_path_pattern = get_config(logger).get('Level2', 'GapsFile')
    gaps_tar_file = run.format(get_config(logger).get('Level2', 'GapsTarFile'))

    l2files = run.get_level2_files()
    gaps_files = [f.get_gaps_file() for f in l2files]

    logger.debug('Gaps files: {0}'.format([f.path for f in gaps_files]))

    tar_files(gaps_files, gaps_tar_file, logger, dryrun, mode = 'w:bz2')

    for f in gaps_files:
        iceprod.remove_file_from_catalog(dataset_id, run, f)

    if dryrun:
        # tar-ed file has alternate location if dryrun:
        gaps_tar_file = os.path.join(path.get_tmpdir(), os.path.basename(gaps_tar_file))

    iceprod.add_file_to_catalog(dataset_id, run, File(gaps_tar_file, logger))

def insert_gaps_file_info_into_db(run, dryrun, logger):
    from databaseconnection import DatabaseConnection
    from config import get_config

    gaps_path_pattern = get_config(logger).get('Level2', 'GapsFile')

    l2files = run.get_level2_files()
    gaps_files = [f.get_gaps_file() for f in l2files]
    gaps_files = [f for f in gaps_files if f.exists()]

    logger.debug('Gaps files: {0}'.format([f.path for f in gaps_files]))

    sub_runs = []
    gaps = []
    for gf in gaps_files:
        logger.debug("File {0}".format(gf.path))

        gf.read()
        sub_runs.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (gf.get_run_id(), \
                                                                  gf.get_sub_run_id(), \
                                                                  gf.get_first_event()['event'], \
                                                                  gf.get_last_event()['event'], \
                                                                  gf.get_first_event()['year'],\
                                                                  gf.get_first_event()['frac'],\
                                                                  gf.get_last_event()['year'],\
                                                                  gf.get_last_event()['frac'],\
                                                                  gf.get_file_livetime()))

        logger.debug("INSERT set: {0}".format(sub_runs[-1]))

        if gf.has_gaps():
            for gap in gf.get_gaps():
                gap_insert_sql = 'INSERT INTO gaps (run_id, sub_run, prev_event_id, curr_event_id, delta_time, prev_event_frac, curr_event_frac) VALUES (%s, %s, %s, %s, %s, %s, %s)'
                gaps.append(gap_insert_sql % (gf.get_run_id(), gf.get_sub_run_id(), gap['prev_event_id'],  gap['curr_event_id'],  gap['dt'],  gap['prev_event_frac'], gap['curr_event_frac']))

    db = DatabaseConnection.get_connection('filter-db', logger)

    if gaps:
        # Insert gaps
        logger.debug('Insert gaps into db')
        sql = ';'.join(gaps)
        logger.debug('SQL: {0}'.format(sql))

        if not dryrun:
            db.execute(sql)

    # Insert sub runs
    sql = """INSERT INTO sub_runs 
                (run_id, sub_run, first_event, last_event, first_event_year, first_event_frac, last_event_year, last_event_frac, livetime)
             VALUES {0}
             ON DUPLICATE KEY UPDATE first_event = VALUES(first_event),
                                     last_event = VALUES(last_event),
                                     first_event_year = VALUES(first_event_year),
                                     first_event_frac = VALUES(first_event_frac),
                                     last_event_year = VALUES(last_event_year),
                                     last_event_frac = VALUES(last_event_frac),
                                     livetime = VALUES(livetime)"""

    logger.debug('Insert sub runs into db')
    sql = sql.format(','.join(sub_runs))

    logger.debug('SQL: {0}'.format(sql))

    if not dryrun:
        db.execute(sql)

    return gaps_files

