
import files
import times
from config import get_config
from icecube import dataclasses, icetray

class LoadRunDataException(Exception):
    pass

class Run(object):
    def __init__(self, run_id, logger, db = None, dryrun = False):
        self.run_id = int(run_id)
        self.logger = logger
        self.dryrun = dryrun

        if db is None:
            from databaseconnection import DatabaseConnection
            self._db = DatabaseConnection.get_connection('filter-db', logger)
            if self._db is None:

                raise Exception('No database connection')
        else:
            self._db = db

        self._data = None
        self._subruns = {'common': None, 'PFDST': None, 'PFFilt': None, 'Level2': None, 'Level2pass2': None}
        self._gcd_file = None
        self._season = None

    def is_test_run(self, force_reload = False):
        """
        Returns `True` if the run is a test run. This is a shortcut for `libs.config.get_config(<logger>).is_test_run(run.run_id)`.

        Note: The database needs to be up to date in order to be able to determine the return value correctly.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
        """

        return get_config(self.logger).is_test_run(self.run_id, force_reload)

    def get_number_of_events(self, force_reload = False):
        """
        Returns the number of events provided by i3live.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: The number of events
        """

        self._load_data(force_reload)
        return int(self._data['nevents'])

    def get_rate(self, force_reload = False):
        """
        Returns the rate provided by i3live.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            float: The rate
        """

        self._load_data(force_reload)
        return float(self._data['rate'])

    def is_good_in_ice_run(self, force_reload = False):
        """
        Checks if this run is marked as good in ice.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            boolean: `True` if it is a good in ice run.
        """

        self._load_data(force_reload)

        return bool(self._data['good_i3'])

    def is_good_ice_top_run(self, force_reload = False):
        """
        Checks if this run is marked as good ice top.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            boolean: `True` if it is a good ice top run.
        """

        self._load_data(force_reload)

        return bool(self._data['good_it'])

    def is_good_run(self, force_reload = False):
        """
        Checks if this run is marked as good ice top or good in ice.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            boolean: `True` if it is a good ice top or good in ice run.
        """

        self._load_data(force_reload)

        return self.is_good_ice_top_run(force_reload) or self.is_good_in_ice_run(force_reload)

    def set_data(self, tstart, tstart_frac, good_tstart, good_tstart_frac, tstop, tstop_frac, good_tstop, good_tstop_frac, good_i3, good_it, snapshot_id, production_version, nevents, rate):
        """
        In order to be able to use this class although this run is not in the production database yet, you need to provide all information
        explicitely.
        """

        # Set common sub run info to a empty dict to avoid auto-loading.
        self._subruns['common'] = {}
        self._data = locals()

    def _load_data(self, force_reload = False):
        """
        Loads the data of the run and sub runs.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
        """

        # Load only data if forced or not loaded yet
        if not force_reload and self._data is not None and self._subruns['common'] is not None:
            return

        # Run information
        sql = 'SELECT * FROM i3filter.runs WHERE run_id = {run_id} ORDER BY production_version DESC LIMIT 1'.format(run_id = self.run_id)
        data = self._db.fetchall(sql)

        if not len(data):
            raise LoadRunDataException('Run {run_id} does not exist in the database.'.format(run_id = self.run_id))

        if len(data) != 1:
            raise LoadRunDataException('Something is wrong with the DB result')

        self._data = data[0]

        # Common sub run information
        sql = 'SELECT * FROM i3filter.sub_runs WHERE run_id = {run_id}'.format(run_id = self.run_id)
        subrun_data = self._db.fetchall(sql)

        sql = 'SELECT * FROM i3filter.gaps WHERE run_id = {run_id}'.format(run_id = self.run_id)
        gaps_data = self._db.fetchall(sql)

        self._subruns['common'] = {}
        for subrun in subrun_data:
            sr = SubRun(None, self.logger)
            sr.sub_run_id = subrun['sub_run']
            sr.run = self
            sr._data = subrun
            sr._data['gaps'] = []

            for gap in gaps_data:
                if gap['sub_run'] == sr.sub_run_id:
                    sr._data['gaps'].append(gap)

            self._subruns['common'][int(sr.sub_run_id)] = sr

    def _calculate_active_x(self, force_reload = False):
        gcd_file = self.get_gcd_file(force_reload)

        if gcd_file is None:
            raise Exception('No GCD file has been found.')

        from icecube import dataio
        f = dataio.I3File(gcd_file.path)

        bdl = None
        while f.more():
            frame = f.pop_frame()
            if frame.Has(get_config().get('GCD', 'BadDomListName')):
                bdl = frame[get_config().get('GCD', 'BadDomListName')]
                break

        if bdl is None:
            raise Exception('No bad dom list has been found in GCDf file.')

        # Make default detector configuration
        detector_conf = {}
        for s in range(0, 87):
            detector_conf[s] = range(1, 67)

        # Remove bad doms
        for dom in bdl:
            detector_conf[d.string].pop(detector_conf[d.string].index(d.om))

        # Calculate values
        # Count number of strings with at least one active non-IceTop DOM
        self._data['active_strings'] = len([k for k in detector_conf.keys() if len(set(detector_conf[k]).difference([61, 62, 63, 64, 65, 66]))])

        # Sum all DOMs after excluding bad DOMs
        self._data['active_doms'] = sum([len(detector_conf[k]) for k in detector_conf.keys()])
        self._data['active_in_ice_doms'] = sum([len(set(detector_conf[k]).difference(set([61, 62, 63, 64, 65, 66]))) for k in detector_conf.keys()])

    def write_active_x_to_db(self):
        """
        Calculates
            * active_strings
            * active_doms
            * active_in_ice_doms
        and writes it to the database. (And updates this object.)
        """

        self._calculate_active_x()
        sql = '''
            UPDATE i3filter.runs
                SET active_strings = {active_strings:d},
                active_doms = {active_doms:d},
                active_in_ice_doms = {active_in_ice_doms:d}
            WHERE
                run_id = {run_id} AND
                production_version = {production_version} AND
                snapshot_id = {snapshot_id}'''.format(
                    run_id = self.run_id,
                    production_version = self._data['production_version'],
                    snapshot_id = self._data['snapshot_id'],
                    active_doms = self._data['active_doms'],
                    active_in_ice_doms = self._data['active_in_ice_doms'],
                    active_strings = self._data['active_strings']
        )

        self.logger.info('Write active doms and strings to DB: active_doms = {active_doms}, active_in_ice_doms = {active_in_ice_doms}, active_strings = {active_strings}'.format(active_doms = self._data['active_doms'], active_in_ice_doms = self._data['active_in_ice_doms'], active_strings = self._data['active_strings']))

        if not self.dyrun:
            self._db.execute(sql)

    def _get_active_x(self, x, force_reload = False):
        self._load_data(force_reload)

        if self._data[x] is None:
            self._calculate_active_x(force_reload)

        return self._data[x]

    def get_active_strings(self, force_reload = False):
        """
        Returns the number of active strings. It tries to use the data form the database.
        If no data is available through the DB, the GCD file will be read.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: Number of active strings
        """

        return self._get_active_x('active_strings', force_reload)

    def get_active_doms(self, force_reload = False):
        """
        Returns the number of active doms. It tries to use the data form the database.
        If no data is available through the DB, the GCD file will be read.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: Number of active doms
        """

        return self._get_active_x('active_doms', force_reload)

    def get_active_in_ice_doms(self, force_reload = False):
        """
        Returns the number of active in ice doms. It tries to use the data form the database.
        If no data is available through the DB, the GCD file will be read.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: Number of active in ice doms
        """

        return self._get_active_x('active_in_ice_doms', force_reload)

    def get_start_time(self, force_reload = False):
        """
        Returns the start time of the run.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            dataclasses.I3Time: The time
        """

        self._load_data(force_reload)
        return times.get_i3time(self._data['tstart'], self._data['tstart_frac'])

    def get_stop_time(self, force_reload = False):
        """
        Returns the stop time of the run.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            dataclasses.I3Time: The time
        """

        self._load_data(force_reload)
        return times.get_i3time(self._data['tstop'], self._data['tstop_frac'])

    def get_good_start_time(self, force_reload = False):
        """
        Returns the good start time of the run.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            dataclasses.I3Time: The time
        """

        self._load_data(force_reload)
        return times.get_i3time(self._data['good_tstart'], self._data['good_tstart_frac'])

    def get_good_stop_time(self, force_reload = False):
        """
        Returns the good stop time of the run.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            dataclasses.I3Time: The time
        """

        self._load_data(force_reload)
        return times.get_i3time(self._data['good_tstop'], self._data['good_tstop_frac'])

    def get_livetime(self, force_reload = False):
        """
        Returns the livetime of the with respect to the gaps.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            float: The livetime
        """

        self._load_data(force_reload)

        if self._subruns is None or not len(self._subruns):
            raise Exception('This run has no sub runs in the database yet. Make sure that you call this method only if this run has been processed successfully.')

        # get_livetime() and get_gaps() do not need a force_reload since it will be forced to reload with the call of self._load_data(force_reload).
        return sum([sr.get_livetime() for sr in self._subruns]) - sum([g['delta_time'] for sr in self._subruns for g in sr.get_gaps()])

    def _get_x_files(self, path_pattern, x, force_reload = False):
        """
        Returns the files of the run of type x while x can be PFFilt, PFDST, Level2, Level2pass2. However, the
        type is actually determined in the configuration file

        Returns:
            dict: `{sub_run_id: SubRun, ...}`
        """

        if self._subruns[x] is None or force_reload:
            from stringmanipulation import replace_var
            from dateutil.relativedelta import relativedelta
            from glob import glob
            from path import get_sub_run_id_from_path

            path_pattern = replace_var(path_pattern, 'sub_run_id', '*')

            next_day = self.get_start_time().date_time + relativedelta(days = 1)

            todays_path = self.format(path_pattern)
            tomorrows_path = self.format(path_pattern, **{'year': next_day.year, 'month': next_day.month, 'day': next_day.day})

            paths = glob(todays_path)
            paths.extend(glob(tomorrows_path))

            result = {get_sub_run_id_from_path(p, x, self.logger): p for p in paths}

            # Create SubRuns
            self._subruns[x] = {}

            for sub_run_id, path in result.items():
                sr = None

                if sub_run_id in self._subruns['common']:
                    sr = self._subruns['common'][sub_run_id].copy()
                    sr.path = path
                else:
                    sr = SubRun(path, self.logger)
                    sr.run = self
                    sr.sub_run_id = sub_run_id

                sr.filetype = x

                self._subruns[x][sub_run_id] = sr

        return self._subruns[x]

    def get_pffilt_files(self, force_reload = False):
        """
        Returns a list of SubRuns of type `PFFilt` (list of PFFilt files).

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            list: List of SubRuns
        """

        return self._get_x_files(get_config(self.logger).get('PFFilt', 'PFFiltFile'), 'PFFilt', force_reload).values()

    def get_pfdst_files(self, force_reload = False):
        """
        Returns a list of SubRuns of type `PFDST` (list of PFDST files).

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            list: List of SubRuns
        """

        return self._get_x_files(get_config(self.logger).get('PFDST', 'PFDSTFile'), 'PFDST', force_reload).values()

    def get_level2_files(self, production_version = None, force_reload = False):
        """
        Returns a list of SubRuns of type `Level2` (list of Level2 files).

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            list: List of SubRuns
        """

        return self._get_x_files(get_config(self.logger).get('Level2', 'Level2File'), 'Level2', force_reload).values()

    def get_level2pass2_files(self, production_version = None, force_reload = False):
        """
        Returns a list of SubRuns of type `Level2pass2` (list of Level2pass2 files).

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            list: List of SubRuns
        """

        return self._get_x_files(get_config(self.logger).get('Level2pass2', 'Level2pass2File'), 'Level2pass2', force_reload).values()

    def set_post_processing_state(self, dataset_id, validated):
        """
        Sets the validation status of this run for the given dataset.

        Args:
            dataset_id (int): The dataset_id
            validated (boolean): The validation status
        """

        sql = """
            INSERT INTO i3filter.post_processing
                (run_id, dataset_id, validated, date_of_validation)
            VALUES
                ({run_id:d}, {dataset_id:d}, {validated:d}, NOW())
            ON DUPLICATE KEY UPDATE
                validated = {validated:d},
                date_of_validation = NOW()
                """.format(
            run_id = self.run_id,
            dataset_id = dataset_id,
            validated = validated
        )

        self.logger.info('Set post processing state for run {run_id} and dataset id {dataset_id} to {validated}'.format(
            run_id = self.run_id,
            dataset_id = dataset_id,
            validated = validated
        ))

        if not self.dryrun:
            self._db.execute(sql)

    def remove_bad_sub_runs(self, force_reload = False):
        pass

    def get_production_version(self, force_reload = False):
        """
        Returns the production version.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: The production version
        """

        self._load_data(force_reload)
        return self._data['production_version']

    def get_snapshot_id(self, force_reload = False):
        """
        Returns the latest snapshot_id of this run.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: Latest snapshit id
        """

        self._load_data(force_reload)

        return self._data['snapshot_id']

    def get_gcd_file(self, force_reload = False):
        """
        Look for GCD files in the following order:
          1. run folder
          2. verified gcd folder
          3. all gcd folder
          4. data files folder

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            files.File: The GCD file. If no GCD file has been found, `None` is returned.
        """

        if not force_reload and self._gcd_file is not None:
            return self._gcd_file

        config = get_config(self.logger)

        paths = [
            config.get('Level2', 'RunFolderGCD'),
            config.get('GCD', 'VerifiedGCDPath'),
            config.get('GCD', 'AllGCDPath'),
            config.get('GCD', 'GCDDataPath'),
        ]

        f = None
        for path in paths:
            f = files.File(self.format(path, force_reload), self.logger)
            if f.exists():
                break
            else:
                f = None

        return f

    def is_validated(self, dataset_id):
        """
        Checks if the run has been validated for the given dataset.

        Args:
            dataset_id (int): The dataset_id

        Returns:
            boolean: `True` if the run has been validated, otherwise `False`.
        """

        sql = 'SELECT * FROM i3filter.post_processing WHERE run_id = {run_id:d} AND dataset_id = {dataset_id:d}'.format(run_id = self.run_id, dataset_id = dataset_id)

        self.logger.debug("SQL: {0}".format(sql))

        result = self._db.fetchall(sql)

        if not result:
            return False

        if len(result) > 1:
            raise Exception('Unexpected DB result. Only one entry expected.')

        return result[0]['validated']

    def get_season(self):
        if self._season is None:
            self._season = get_config(self.logger).get_season_by_run(self.run_id)
            if self._season == -1:
                raise Exception('The season of run {0} has not been determined'.format(self.run_id))

        return self._season

    def format(self, path, force_reload = False, **kwargs):
        """
        Formats paths or strings with the information of this run. It provides the values for the following data:
            * run_id
            * year
            * month
            * day
            * season
            * production_version
            * snapshot_id
            * now (the current time)

        Args:
            path (str): The path or string
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
        """

        import datetime

        self._load_data(force_reload)

        if 'run_id' not in kwargs:
            kwargs['run_id'] =self.run_id
        
        if 'ywar' not in kwargs:
            kwargs['year'] =self._data['tstart'].year
        
        if 'month' not in kwargs:
            kwargs['month'] =self._data['tstart'].month
        
        if 'day' not in kwargs:
            kwargs['day'] =self._data['tstart'].day
        
        if 'season' not in kwargs:
            kwargs['season'] =self.get_season()
        
        if 'production_version' not in kwargs:
            kwargs['production_version'] =self._data['production_version']
        
        if 'snapshot_id' not in kwargs:
            kwargs['snapshot_id'] =self._data['snapshot_id']
        
        if 'now' not in kwargs:
            kwargs['now'] =datetime.datetime.now()

        return path.format(**kwargs)

class SubRun(files.File):
    def __init__(self, path, logger):
        super(SubRun, self).__init__(path, logger)

        self.run = None
        self.sub_run_id = None
        self.filetype = None
        self._data = None

    def copy(self):
        """
        Creates a copy of the subrun.

        Returns:
            SubRun: The copy
        """

        copy = SubRun(self.path, self.logger)
        copy.run = self.run
        copy.sub_run_id = self.sub_run_id
        copy.filetype = self.filetype
        copy._data = self._data

        return copy

    def get_start_time(self):
        """
        Returns the date of the first event.

        Returns:
            dataclasses.I3Time: Time of first event
        """

        return dataclasses.I3Time(self._data['first_event_year'], self._data['first_event_frac'])

    def get_stop_time(self):
        """
        Returns the date of the end of the last event.

        Returns:
            dataclasses.I3Time: Time of the end of the last event
        """

        return dataclasses.I3Time(self._data['last_event_year'], self._data['last_event_frac'])

    def get_first_event(self):
        """
        Returns the first event number.

        Returns:
            int: Event number
        """

        return self._data['first_event']

    def get_last_event(self):
        """
        Returns the last event number.

        Returns:
            int: Event number
        """

        return self._data['last_event']

    def get_livetime(self):
        """
        Returns the livetime of the sub run.

        Note: Gaps are not taken into account!

        Returns:
            float: The livetime in seconds.
        """

        return self._data['livetime']

    def get_gaps(self):
        """
        Returns a list of gaps.

        Each list entry is a dict with the following keys:
            * prev_event_id
            * curr_event_id
            * delta_time
            * prev_event_frac
            * curr_event_frac

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            list: List of dicts
        """

        return self._data['gaps']

def validate_file_integrity(files, logger, run_start_time = None, run_stop_time = None, show_mismatches = True, detailed_info = {}):
    """
    Checks if the files are OK. Only PFDST and PFFilt files!

    Args:
        files (list): List of files. Usually use Run.get_pffilt_files() or similar. Only PFDST or PFFilt files!
        logger (Logger): The logger
        run_start_time (datetime.datetime): If the run to that the files belong is not in the DB yet, you need to provide the good run start time.
        run_stop_time (datetime.datetime): If the run to that the files belong is not in the DB yet, you need to provide the good run stop time.
        show_mismatches (boolean): Print time mismatches. E.g. for bad runs it is not important to see mismatches.
        detailed_info (dict): If one wants to have detailed infos of the performed checks provide a dict.

    Returns:
        boolean: `True` if everything is OK
    """

    from icecube import dataclasses

    for f in files:
        if f.filetype not in ['PFDST', 'PFFilt']:
            raise Exception('This function only works with PFDST and PFFilt files')

    if not len(files):
        logger.warning('Input file list is empty, maybe failed/short run')
        return False

    # Check if good start/stop times are available:
    try:
        run_start_time = files[0].run.get_good_start_time()
        run_stop_time = files[0].run.get_good_stop_time()
    except LoadRunDataException as e:
        # OK, the run is not in the DB yet. Check if the times are provided
        if run_start_time is None or run_stop_time is None:
            raise Exception('Run {run_id} is not in the offline production DB yet. Therefore, you need to provide the good start/stop times explicitely')

    if isinstance(run_start_time, dataclasses.I3Time):
        run_start_time = run_start_time.date_time

    if isinstance(run_stop_time, dataclasses.I3Time):
        run_stop_time = run_stop_time.date_time

    run_start_time = run_start_time.replace(microsecond = 0)
    run_stop_time = run_stop_time.replace(microsecond = 0)

    # We have the start/stop times. Keep going
    detailed_info[files[0].run.run_id] = {'missing_files': [], 'metadata_start_time': None, 'metadata_stop_time': None, 'empty_files': [], 'wrong_permission': []}
    detailed_info_element = detailed_info[files[0].run.run_id]

    files.sort(key = lambda sr: sr.sub_run_id)

    # Check if we have all files. That means if there is a gap between the first and last file
    if len(files) != files[-1].sub_run_id - files[0].sub_run_id + 1:
        logger.warning('We have missing files')

        # Ok, find missing parts
        missing_files = list(set(range(files[0].sub_run_id, files[-1].sub_run_id + 1)) - set([sr.sub_run_id for sr in files])).sort()
        logger.warning('{0} files are missing:'.format(len(missing_files)))
        for i, m in enumerate(missing_files):
            logger.warning('{index:>4}. Part {num}'.format(index = i + 1, num = m))

        detailed_info_element['missing_files'] = missing_files

        return False

    # Check time start/stop: i3live times (the data in the DB) and the times in the metadata of the run files
    import tarfile
    import os

    from xml.etree.ElementTree import ElementTree
    from dateutil.parser import parse

    def get_meta_xml(f):
        tfile = tarfile.open(f)
        for member in tfile.getmembers():
            if '.meta.xml' in member.name:
                return tfile.extractfile(member), tfile

        return None, tfile

    # Times of run
    start_time = None
    stop_time = None

    # First file
    logger.info("Check start time")
    meta_xml, tfile = get_meta_xml(files[0].path)

    if meta_xml is None:
        raise Exception('Could not find *.meta.xml in tar file')

    doc = ElementTree(file = meta_xml)
    start_time = [t.text for t in doc.getiterator() if t.tag=="Start_DateTime"]

    tfile.close()

    if not len(start_time):
        raise Exception('Could not find start time')

    start_time = parse(start_time[0])

    # Last file
    logger.info("Check stop time")
    meta_xml, tfile = get_meta_xml(files[-1].path)

    if meta_xml is None:
        raise Exception('Could not find *.meta.xml in tar file')

    doc = ElementTree(file = meta_xml)
    stop_time = [t.text for t in doc.getiterator() if t.tag=="End_DateTime"]

    tfile.close()

    if not len(stop_time):
        raise Exception('Could not find stop time')

    stop_time = parse(stop_time[0])

    mismatches = False
    if start_time > run_start_time:
        if show_mismatches:
            logger.warning('Mismatch in start time. Reported by i3Live: {i3live}; file metadata: {meta}'.format(i3live = run_start_time, meta = start_time))
            logger.warning('File: {0}'.format(files[0].path))

        mismatches = True

    if stop_time < run_stop_time:
        if show_mismatches:
            logger.warning('Mismatch in end time. Reported by i3Live: {i3live}; file metadata: {meta}'.format(i3live = run_stop_time, meta = stop_time))
            logger.warning('File: {0}'.format(files[-1].path))

        mismatches = True

    detailed_info_element['metadata_start_time'] = start_time
    detailed_info_element['metadata_stop_time'] = stop_time

    # Check file size and file permissions
    import stat

    logger.info("Check file size and file permissions")

    for f in files:
        if f.size() == 0:
            detailed_info_element['empty_files'].append(f.path)
            logger.debug('Found empty file: {0}'.format(f.path))

        if not (os.stat(f.path).st_mode & stat.S_IRGRP):
            detailed_info_element['wrong_permission'].append(f.path)
            logger.debug('Found file with wrong permissions: {0}'.format(f.path))

    return (not mismatches) and len(detailed_info_element['empty_files']) == 0 and len(detailed_info_element['wrong_permission']) == 0




