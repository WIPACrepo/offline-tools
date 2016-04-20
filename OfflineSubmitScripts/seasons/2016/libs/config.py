
import json
import os
import ConfigParser
import files
import collections
import re

class ExtParser(ConfigParser.SafeConfigParser):
    """
    Implements the possibility to include variables from other sections: ${Section:var}
    This class has been found here: http://stackoverflow.com/a/35877548
    """

    def __init__(self, *args, **kwargs):
        self.cur_depth = 0 
        ConfigParser.SafeConfigParser.__init__(self, *args, **kwargs)

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

def get_config():
    """
    Returns the config parser for offline processing. It reads
    the config file in `config/offline_processing.cfg`.

    Note: The config parser is only created once. Any other time you
    Call this function it returns the same instance.

    Returns:
        ConfigParser.SafeConfigParser: The config parser
    """
    if not hasattr(get_config, 'configparser'):
        get_config.configparser = ExtParser({
            'tmpdir': files.get_tmpdir(),
            'rootdir': files.get_rootdir(),
            'logdir': files.get_logdir()
        })
        get_config.configparser.read(os.path.join(files.get_rootdir(), 'config', 'offline_processing.cfg'))
    
    return get_config.configparser

def get_var_dict(section, name, keytype = str, valtype = str):
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
    config = get_config()

    vars = config.items(section)

    varbeginning = str(name)

    vars_dict = {}

    for v in vars:
        # To make it possible that the keys 'nameXXX', ... and 'name' are existing,
        # the length of the key has to be checked
        if len(v[0]) > len(varbeginning) and v[0][:len(varbeginning)].lower() == varbeginning.lower():
            key = keytype(v[0][len(varbeginning):])

            if type(valtype) == str and valtype.lower() == 'json':
                vars_dict[key] = json.loads(v[1])
            else:
                vars_dict[key] = valtype(v[1])

    return collections.OrderedDict(sorted(vars_dict.items()))

def get_var_list(section, name, valtype = str):
    """
    It is equal to `get_var_dict` but returns a list with the values and doesn't use the keys.
    For further informations check `get_var_dict`.

    Args:
        section (str): The section
        name (str): The name of the var. That means, the begging of the config key that is followed by some other characters
        valtyoe (type|json): Default is `str`. Tries to convert the value to the given type. If the string `'json'` is passed, it json parses the value.
    """

    val_list = []
    val_dict = get_var_dict(section = section, name = name, valtype = valtype)

    for k, v in val_dict.iteritems():
        val_list.append(v)

    return val_list

def get_seasons_info():
    """
    Returns the available informations of the seasons that are stored in the config file
    as an `OrderedDict`. The key is an integer that represents the year/season; the value is
    an dict that contains the keys `first` and `test`. `first` indicates the first run number and
    `test` is a list of runs that are the test runs.

    Note: The dict is ordered by the year number ascending.

    Note: It is a shortcut for `get_var_dict(section = 'DEFAULT', name = 'Season', keytype = int, valtype = 'json')`.

    Returns:
        collection.OrderedDict: The dict
    """

    return get_var_dict(section = 'DEFAULT', name = 'Season', keytype = int, valtype = 'json')

def get_season_info(season):
    """
    Returns the info to the given season. It is an shortcut for
    `get_seasons_info()[season]`.

    It raises an exception if no info for this season is available.

    Args:
        season (int): The season

    Returns:
        dict: All info about the given season.
    """

    return get_seasons_info()[int(season)]

def get_season_by_run(run_id):
    """
    Returns the season identified by the run number with respect to the test runs.

    Args:
        run_id (int): Run number

    Returns:
        int: Season. If no season found, -1 will be returned.
    """
    seasons = get_seasons_info()

    found_season = -1
    for s, v in seasons.iteritems():
        if (run_id >= v['first'] and v['first'] != -1) or run_id in v['test']:
            found_season = s

        if run_id < v['first'] and found_season > -1:
            return found_season

    return found_season

if __name__ == '__main__':
    config = get_config()

    print config.get('PoleGCDChecks', 'VerifiedGCDsPath')
    print config.get('GCDGeneration', 'SpeCorrectionFile')
    print config.get('GCDGeneration', 'TmpCondorSubmitFile')

    print get_var_list('TemplateGCDChecks', 'NotificationReceiver')
    print get_var_dict('TemplateGCDChecks', 'NotificationReceiver')

    print get_seasons_info()
    print get_var_dict(section = 'DEFAULT', name = 'Season', keytype = str, valtype = str)

    print get_season_by_run(127390)
    print get_season_by_run(126290)
    print get_season_by_run(126296)

    print config.get('GCDGeneration', 'SpeCorrectionFile')

