
import os

def remove_path_prefix(path):
    """
    Removes `file:` or `gsiftp://gridftp.icecube.wisc.edu` from path.
    """

    prefix = ['file:', 'gsiftp://gridftp.icecube.wisc.edu']

    for p in prefix:
        if path.startswith(p):
            return path[len(p):]

    return path

def get_tmpdir():
    """
    Get the tmpdir under the root path

    Returns:
        str
    """

    root_dir = get_rootdir()
    return os.path.join(root_dir, "tmp")

def get_logdir(sublogpath = ""):
    """
    Get the root log dir

    Keyword Args:
        sublogpath (str): path under ../log

    Returns:
        str
    """

    root_dir = get_rootdir()
    return os.path.join(root_dir, os.path.join("logs", sublogpath))

def get_rootdir():
    """
    Get the absolute path of the OfflineSubmitScripts_whatever
    FIXME: This is sensitive to the location of its implementation
    """

    thisdir = os.path.dirname(os.path.abspath(__file__))
    rootdir = os.path.split(thisdir)[0] # go one up
    return rootdir

def get_bindir():
    """
    Get the absolute path of the ./bin/ folder.

    Return:
        str: The path
    """

    return os.path.join(get_rootdir(), 'bin')

def get_env_python_path():
    """
    Returns the absolite path to the ./bin/EnvPython.sh script that
    is equivalent to `/path/to/build/./env-shell.sh python`.

    Returns:
        str: The path
    """

    return os.path.join(get_bindir(), 'EnvPython.sh')

def get_sub_run_id_from_path(path, pattern, logger):
    """
    The pattern is generated with stringmanipulation.make_regex_for_var.

    Args:
        path (str): The path
        pattern (str): The path pattern
        logger (Logger): The logger

    Returns:
        int: sub run number
    """

    import re
    from stringmanipulation import make_regex_for_var

    regex = make_regex_for_var(pattern, 'sub_run_id', ignored_var_names = '*', var_value = '[0-9]+')

    logger.debug('Path = {path}, regex = {regex}'.format(path = path, regex = regex))

    c = re.compile(regex)
    return int(c.search(path).groups()[0])

def make_relative_symlink(source, link_name, dryrun, logger, replace = False):
    rel_source = os.path.relpath(source, os.path.dirname(link_name))

    logger.debug('rel_source = {0}'.format(rel_source))
    logger.debug('link_name = {0}'.format(link_name))

    if not dryrun:
        if replace and os.path.exists(link_name):
            logger.warning('Link name {0} already exists. Will replace it.'.format(link_name))
            os.remove(link_name)

        os.symlink(rel_source, link_name)

def get_condor_scratch_folder(default = None):
    """
    Tries to get the _CONDOR_SCRATCH_DIR. If not available or not writable, `default` will be returned.

    Args:
        default (str): The value if the scratch is not available. Default is `None`.

    Returns:
        str: The path or `default` if not available.
    """

    try:
        if os.access(os.environ["_CONDOR_SCRATCH_DIR"], os.W_OK):
            return os.environ["_CONDOR_SCRATCH_DIR"]
    except:
        return default

