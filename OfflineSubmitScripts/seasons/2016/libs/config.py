
import json
import os
import ConfigParser
import files

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
        get_config.configparser = ConfigParser.SafeConfigParser({
            'tmpdir': files.get_tmpdir(),
            'rootdir': files.get_rootdir(),
            'logdir': files.get_logdir()
        })
        get_config.configparser.read(os.path.join(files.get_rootdir(), 'config', 'offline_processing.cfg'))
    
    return get_config.configparser

if __name__ == '__main__':
    config = get_config()

    print config.get('PoleGCDChecks', 'VerifiedGCDsPath')
    print json.loads(config.get('PoleGCDChecks', 'NotificationReceivers'))
    print config.get('GCDGeneration', 'SpeCorrectionFile')
    print config.get('GCDGeneration', 'TmpCondorSubmitFile')

