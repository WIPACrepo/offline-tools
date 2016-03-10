#!/usr/bin/env python

import os, sys
import datetime
from dateutil.relativedelta import relativedelta
import cPickle
import json
import smtplib


def CreateMsg(domain,sender,receivers, subject,messageBody, mimeVersion="",contentType=""):
    try:

        message = "From: " + sender + "<" + sender+domain + ">\n"
        message += "To: " + ",".join([r[0:r.find('@')]+ "<%s>"%r for r in receivers]) + "\n"

        if len(mimeVersion) : message += "MIME-Version: "+ mimeVersion +"\n"
        if len(contentType) : message += "Content-type: " + contentType +"\n"

        message += "Subject: " + subject + "\n\n"

        message += messageBody

        return message

    except Exception,err:
        raise Exception("Error: %s"%str(err))

def SendMsg(sender,receivers,message):
    try:
        #smtpObj = smtplib.SMTP('localhost')
        smtpObj = smtplib.SMTP('mail.icecube.wisc.edu')
        smtpObj.sendmail(sender, receivers, message)
        print "Successfully sent notification email"
    except Exception,err:
        raise Exception("Error: %s"%str(err))