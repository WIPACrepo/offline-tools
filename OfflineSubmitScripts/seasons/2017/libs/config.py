
import json
import os
import ConfigParser
import path
import collections
import re
from databaseconnection import DatabaseConnection

class Config(ConfigParser.SafeConfigParser):
    """
    Implements the possibility to include variables from other sections: ${Section:var}
    This class has been found here: http://stackoverflow.com/a/35877548
    """

    def __init__(self, *args, **kwargs):
        self.cur_depth = 0 
        ConfigParser.SafeConfigParser.__init__(self, *args, **kwargs)
        self.logger = None

        self.db = None
        self.season_info = None
        self.dataset_info = None
        self.level3_info = None

    def get(self, section, option, raw=False, vars=None):
        r_opt = ConfigParser.SafeConfigParser.get(self, section, option, raw=True, vars=vars)
        if raw:
            return r_opt

        ret = r_opt
        re_oldintp = r'%\((\w*)\)s'
        re_newintp = r'\$\{(\w*):(\w*)\}'

        m_new = re.findall(re_newintp, r_opt)
        if m_new:
            for f_section, f_option in m_new:
                self.cur_depth = self.cur_depth + 1 
                if self.cur_depth < ConfigParser.MAX_INTERPOLATION_DEPTH:
                    sub = self.get(f_section, f_option, vars=vars)
                    ret = ret.replace('${{{0}:{1}}}'.format(f_section, f_option), sub)
                else:
                    raise ConfigParser.InterpolationDepthError, (option, section, r_opt)

        m_old = re.findall(re_oldintp, r_opt)
        if m_old:
            for l_option in m_old:
                self.cur_depth = self.cur_depth + 1 
                if self.cur_depth < ConfigParser.MAX_INTERPOLATION_DEPTH:
                    sub = self.get(section, l_option, vars=vars)
                    ret = ret.replace('%({0})s'.format(l_option), sub)
                else:
                    raise ConfigParser.InterpolationDepthError, (option, section, r_opt)

        self.cur_depth = self.cur_depth - 1 
        return ret

    def get_var_dict(self, section, name, keytype = str, valtype = str):
        """
        Returns the available informations of all keys in `section` that start with `name`
        as an `OrderedDict`. The key is an string that followes the `name`; the value is
        the corresponding config value.

        Example:
        ```
        # config.cfg
        [eMailAddresses]
        mail1 = first@example.com
        mail2 = second@example.com
        mailBatman = bruce@example.com
        ```

        ```
        # example.py
        mails = config.get_var_array('emailAddresses', 'mail')

        for v, k in mails.iteritems():
            print "%s: %s" % (v, k)

        # Output:
        # 1: first@example.com
        # 2: second@example.com
        # Batman: bruce@example.com
        ```

        Note: The dict is ordered by the key ascending.

        Args:
            section (str): The section
            name (str): The name of the var. That means, the begging of the config key that is followed by some other characters
            valtyoe (type|json): Default is `str`. Tries to convert the value to the given type. If the string `'json'` is passed, it json parses the value.
            keytype (type): Default is `str`. Tries to convert the key into the given type.

        Returns:
            collection.OrderedDict: The dict
        """

        vars = self.items(section)

        varbeginning = str(name)

        vars_dict = {}

        for v in vars:
            # To make it possible that the keys 'nameXXX', ... and 'name' are existing,
            # the length of the key has to be checked
            if v[0].lower().startswith(varbeginning.lower()):
                key = keytype(v[0][len(varbeginning):])

                if type(valtype) == str and valtype.lower() == 'json':
                    vars_dict[key] = json.loads(v[1])
                else:
                    vars_dict[key] = valtype(self.get(section, v[0]))

        return collections.OrderedDict(sorted(vars_dict.items()))

    def get_var_list(self, section, name, valtype = str):
        """
        It is equal to `get_var_dict` but returns a list with the values and doesn't use the keys.
        For further informations check `get_var_dict`.

        Args:
            section (str): The section
            name (str): The name of the var. That means, the begging of the config key that is followed by some other characters
            valtype (type|json): Default is `str`. Tries to convert the value to the given type. If the string `'json'` is passed, it json parses the value.
        """

        val_list = []
        val_dict = self.get_var_dict(section = section, name = name, valtype = valtype)

        for k, v in val_dict.iteritems():
            val_list.append(v)

        return val_list

    def get_l2_path_pattern(self, season, ftype, pass_number = 1):
        """
        Returns the path pattern for the given season, pass and file type.

        If no path pattern has been found, an Exception is raised.

        Args:
            season (int): The season, e.g. 2015
            pass_number (int): Usually it is pass_number = 1 (default)
            ftype (str): The file type: `DATA` (L2 files), `EHE`, `GCD`, `ICE_TOP`, `SLOP`, `RUN_FOLDER_GCD`

        Returns:
            str: The path pattern.
        """

        if ftype not in ['DATA', 'ICE_TOP', 'SLOP', 'EHE', 'GCD', 'RUN_FOLDER_GCD', 'GAPS', 'RUN_FOLDER', 'RUN_FOLDER_LINK', 'BAD_FILE_FOLDER']:
            raise Exception('Unknown file type')

        try:
            section = None

            if pass_number == 1:
                if ftype == 'GCD':
                    section = 'GCD'
                else:
                    section = 'Level2'
            elif pass_number == 2:
                if ftype == 'GCDPass2':
                    section = 'GCD'
                else:
                    section = 'Level2pass2'
            else:
                self.logger.warning('Pass {0} is not supported!'.format(pass_number))

            if section is not None:
                value = self.get(section, ftype)
                self.logger.warning('*** OVERRIDDEN VALUE {0} ***'.format(ftype))
                return value

        except ConfigParser.NoOptionError as e:
            # OK, the value has not been overridden within the donfig file. Let's proceed
            pass

        season = int(season)
        pass_number = int(pass_number)

        if self.db is None:
            self.db = DatabaseConnection.get_connection('filter-db', self.logger);

        sql = "SELECT * FROM i3filter.level2_paths WHERE `season` = {season} AND `pass` = {pass_number} AND `type` = '{ftype}'".format(season = season, pass_number = pass_number, ftype = ftype)

        result = self.db.fetchall(sql)
        if not len(result):
            raise Exception('Did not find pattern')

        return result[0]['pattern']

    def get_seasons_info(self, force_reload = False):
        """
        Returns the available informations of the seasons that are stored in the database
        as an `OrderedDict`. The key is an integer that represents the year/season; the value is
        an dict that contains the keys `first` and `test`. `first` indicates the first run number and
        `test` is a list of runs that are the test runs.

        Note: The dict is ordered by the year number ascending.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            collection.OrderedDict: The dict
        """

        if self.season_info is None or force_reload:
            if self.db is None:
                self.db = DatabaseConnection.get_connection('filter-db', self.logger);

            self.logger.debug('Load data')

            data = self.db.fetchall('SELECT * FROM i3filter.seasons')

            seasons = {}

            for row in data:
                self.logger.debug("Season data: {0}".format(row))

                testruns = []

                if row['test_runs'] is not None:
                    testruns = [int(r) for r in row['test_runs'].split(',')]

                seasons[int(row['season'])] = {'first': row['first_run'], 'test': testruns, 'detector_config': row['detector_config']}

            self.season_info = collections.OrderedDict(sorted(seasons.items()))

        return self.season_info

    def get_datasets_info(self, force_reload = False):
        """
        Returns information about all datasets that are in the database. The returned object is an OrderedDict that contains as
        the keys the dataset id. To combine it with season info, look for the 'season' value.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            collection.OrderedDict: The dict with dataset ids as keys
        """

        if self.dataset_info is None or force_reload:
            if self.db is None:
                self.db = DatabaseConnection.get_connection('filter-db', self.logger);

            self.logger.debug('Load data')

            data = self.db.fetchall('SELECT * FROM i3filter.datasets')

            datasets = {}

            for row in data:
                datasets[int(row['dataset_id'])] = row

            self.dataset_info = collections.OrderedDict(sorted(datasets.items()))

        return self.dataset_info

    def get_season_info(self, season = None, force_reload = False):
        """
        Returns the info to the given season. It is an shortcut for
        `get_seasons_info()[season]`.

        It raises an exception if no info for this season is available.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
            season (int): The season. If no season is passed (default) the current season is determined by the `Season` key
                            in the `DEFAULT` section of the config file.

        Returns:
            dict: All info about the given season.
        """

        if season is None:
            season = self.getint('DEFAULT', 'Season')

        return self.get_seasons_info(force_reload = force_reload)[int(season)]

    def get_dataset_info(self, dataset_id, force_reload = False):
        """
        Returns the info to the given dataset. It is an shortcut for
        `get_datasets_info()[dataset_id]`.

        It raises an exception if no info for this `dataset_id` is available.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
            dataset_id (int): The dataset id

        Returns:
            dict: All info about the given `dataset_id`.
        """

        return self.get_datasets_info(force_reload = force_reload)[int(dataset_id)]

    def get_level3_info(self, force_reload = False):
        """
        Returns information about all L3 datasets that are in the database. The returned object is an OrderedDict that contains as
        the keys the dataset id. To combine it with season info, look for the 'season' value.

        Args:
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            collection.OrderedDict: The dict with dataset ids as keys
        """

        if self.level3_info is None or force_reload:
            if self.db is None:
                self.db = DatabaseConnection.get_connection('filter-db', self.logger);

            self.logger.debug('Load data')

            data = self.db.fetchall("SELECT * FROM i3filter.datasets ds JOIN i3filter.level3_outdir l3 ON ds.dataset_id = l3.dataset_id WHERE type = 'L3'")

            datasets = {}

            for row in data:
                datasets[int(row['dataset_id'])] = row

            self.level3_info = collections.OrderedDict(sorted(datasets.items()))

        return self.level3_info

    def get_source_dataset_ids(self, dataset_id):
        """
        Get all parent dataset ids for the given dataset id. The dataset ids are ordered by priority: The first one has the highest priority.

        Note: L2 datasets do not have a parent dataset id.

        Args:
            dataset_id (int): The dataset id

        Returns:
            list: The parent dataset ids.
        """

        if self.db is None:
            self.db = DatabaseConnection.get_connection('filter-db', self.logger);

        data = self.db.fetchall('SELECT * FROM i3filter.source_dataset_id WHERE dataset_id = {0} ORDER BY priority, source_dataset_id'.format(dataset_id))

        return [int(r['source_dataset_id']) for r in data]

    def get_season_by_run(self, run_id, force_reload = False):
        """
        Returns the season identified by the run number with respect to the test runs.

        Args:
            run_id (int): Run number
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            int: Season. If no season found, -1 will be returned.
        """
        seasons = self.get_seasons_info(force_reload = force_reload)

        found_season = -1
        for s, v in seasons.iteritems():
            if (run_id >= v['first'] and v['first'] != -1) or run_id in v['test']:
                found_season = s

            if run_id < v['first'] and found_season > -1:
                return found_season

        return found_season

    def get_dataset_id_by_run(self, run_id, dataset_type = None, force_reload = False):
        """
        Returns the dataset ids identified by the run number with respect to the test runs.

        Args:
            run_id (int): Run number
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.
            dataset_type (str): L2, L3, SDST, or `None` (does not filter by type)

        Returns:
            list: Datset ids. If no dataset id found, the list will be empty.
        """
        datasets = self.get_datasets_info(force_reload = force_reload)
        season = self.get_season_by_run(run_id, force_reload)

        found_dataset = []

        for dataset_id, dataset_info in datasets.items():
            if dataset_type:
                if dataset_info['type'] != dataset_type:
                    continue

            if dataset_info['season'] == season:
                found_dataset.append(dataset_id)

        return found_dataset

    def is_test_run(self, run_id, force_reload = False):
        """
        Checks if the given run is a test run

        Args:
            run_id (int): The run id
            force_reload (boolean): If `True`, no cached data will be used. The default is `False` and usally the value does not change within a script.

        Returns:
            bool: `True` if the run is a test run
        """
        season = self.get_season_by_run(run_id, force_reload)
        season_info = self.get_season_info(season, force_reload)

        return run_id in season_info['test']

def get_config(logger):
    """
    Returns the config parser for offline processing. It reads
    the config file in `config/offline_processing.cfg`.

    Note: The config parser is only created once. Any other time you
    Call this function it returns the same instance.

    Args:
        logger (Logger): The logger

    Returns:
        ConfigParser.SafeConfigParser: The config parser
    """
    if not hasattr(get_config, 'configparser'):
        get_config.configparser = Config({
            'tmpdir': path.get_tmpdir(),
            'rootdir': path.get_rootdir(),
            'logdir': path.get_logdir()
        })

        get_config.configparser.logger = logger

        get_config.configparser.read(os.path.join(path.get_rootdir(), 'config', 'offline_processing.cfg'))
    
    return get_config.configparser


