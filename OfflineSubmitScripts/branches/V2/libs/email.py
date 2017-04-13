
import smtplib
from email.utils import formatdate

def create_message(logger, domain, sender, receivers, subject, message_body, mime_version = '', content_type = ''):
    logger.debug("domain = {domain}, sender = {sender}, receivers = {receivers}, subject = {subject}, message_body = {message_body}, mime_version = {mime_version}, content_type = {content_type}".format(domain = domain, sender = sender, receivers = receivers, subject = subject, message_body = message_body, mime_version = mime_version, content_type = content_type))

    message = "From: " + sender + "<" + sender + domain + ">\n"
    message += "To: " + ",".join([r[0: r.find('@')] + "<{0}>".format(r) for r in receivers]) + "\n"

    if len(mimeVersion):
        message += "MIME-Version: " + mime_version + "\n"

    if len(contentType):
        message += "Content-type: " + content_type + "\n"

    message += "Date: {0}\n".format(formatdate())
    message += "Subject: " + subject + "\n\n"
    message += message_body

    return message

def send_message(sender, receivers, message, logger):
    smtpObj = smtplib.SMTP('mail.icecube.wisc.edu')
    smtpObj.sendmail(sender, receivers, message)

    logger.info("Successfully sent notification email")

def send_email(receivers, subject, content, logger, dryrun):
    from config import get_config

    config = get_config(logger)

    message = create_message(
                        logger,
                        config.get('Notifications', 'eMailDomain'),
                        config.get('Notifications', 'eMailSender'),
                        receivers,
                        subject,
                        content,
                        config.get('Notifications', 'eMailMimeVersion'),
                        config.get('Notifications', 'eMailContentType')
    )

    if not dryrun:
        send_message(config.get('Notifications', 'eMailSender'), receivers, message, logger)
