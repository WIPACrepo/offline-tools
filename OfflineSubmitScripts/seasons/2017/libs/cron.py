
from config import get_config
from libs.utils import Counter
from email import send_email

import logging

def cron_finished(cron_name, counter, logger, dryrun):
    """
    Sends an email with cron_name in subject. It adds the first and last lines of the log file as well as the path to the log file.
    """

    config = get_config(logger)

    filehandlers = [handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)]
    log_files = [h.baseFilename.replace('/mnt/lfs3/', '/data/') for h in filehandlers]

    subject = None
    content = None

    first_lines = config.getint('Crons', 'FirstLines')
    last_lines = config.getint('Crons', 'LastLines')

    if not isinstance(counter, Counter):
        logger.error('Invalid counter object. Sending just a notification with log file path.')

        subject = 'Cron {} finished (missing counter object)'.format(cron_name)
        content = 'Log files: {}'.format(', '.join(log_files))
    else:
        with open(log_files[0], 'r') as log_file:
            lines = log_file.readlines()

        subject = 'Cron {cron_name} finished ({summary})'.format(cron_name = cron_name, summary = counter.get_summary())
        content = 'Cron <b>{cron_name}</b> finished.\n\n{summary}\n\n'.format(cron_name = cron_name, summary = counter.get_summary())
        content += '<b>Log files:</b> <a href="http://convey.icecube.wisc.edu{0}" target="_blank">{0}</a>\n\n'.format(', '.join(log_files))
        content += '<b>First {} lines of the first log file:</b>\n'.format(first_lines)
        content += '<hr>\n'
        content += ''.join(lines[:first_lines])
        content += '...\n'
        content += '<hr>\n'
        content += '<b>Last {} lines of the first log file:</b>\n'.format(first_lines)
        content += '<hr>\n'
        content += '...\n'
        content += ''.join(lines[-last_lines:])
        content += '<hr>\n'

        content = content.replace('\n', '<br>\n')

    receivers = config.get_var_list('Crons', 'NotificationReceiver')

    send_email(receivers, subject, content, logger, dryrun)

