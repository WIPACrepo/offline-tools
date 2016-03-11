
import os

def get_rootdir():
    """
    Get the absolute path of the OfflineSubmitScripts_whatever
    FIXME: This is sensitive to the location of its implementation
    

    """
    thisdir = os.path.split(os.path.abspath(__file__))[0]
    rootdir = os.path.split(thisdir)[0] # go one up
    return rootdir


def get_logdir(sublogpath=""):
    """
    Get the root log dir

    Keyword Args:
        sublogpath (str): path under ../log

    """
    root_dir = get_rootdir()
    return os.path.join(root_dir,os.path.join("logs",sublogpath))


def get_tmpdir():
    """
    Get the tmpdir under the root path
    """

    root_dir = get_rootdir()
    return os.path.join(root_dir,"tmp")

if __name__ == "__main__":
    for i in [get_rootdir(),get_logdir(),get_tmpdir()]:
        print i, os.path.exists(i)
