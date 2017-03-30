
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

def get_sub_run_id_from_path(path, ptype, logger):
    """
    The pattern is gathered from the config file. The `ptype` matches the section. The name of the pattern must be `RegExpForSubRunId`.

    Args:
        ptype (str): Level2, Level2pass2, PFFilt, PFDST
        logger (Logger): The logger

    Returns:
        int: sub run number
    """

    import re
    from config import get_config

    c = re.compile(get_config(logger).get(ptype, 'RegExpForSubRunId'))
    return int(c.search(path).groups()[0])

def make_relative_symlink(source, link_name, dryrun = False, logger = None):
    rel_source = os.path.relpath(source, os.path.dirname(link_name))

    if logger is not None:
        logger.debug('rel_source = {0}'.format(rel_source))

    if not dryrun:
        os.symlink(rel_source, link_name)

