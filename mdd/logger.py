#!/usr/bin/env python
# coding: utf-8

# ## logger
# 
# New notebook

# In[1]:


# import
import logging
import logging.config
import logging.handlers

from env.mdd.metadata import *


# In[3]:


class Logger:
    def __init__(self, metadata_log_yml, log_folder, log_file_name, log_timestamp, debug = False):
        if debug:
            print("##debug begin: class Logger.init")
            print(f"metadata_log_yml: {metadata_log_yml}")

        self.debug = debug

        log_metadata = Metadata_Log(metadata_log_yml, log_folder, log_timestamp, log_file_name)

        self.config = log_metadata.get_config()
        if debug:
            print(f"config: {self.config}")

        logging.config.dictConfig(self.config)

        logger = logging.getLogger("mdd")
        self.logger = logger


# In[ ]:


class TlsSMTPHandler(logging.handlers.SMTPHandler):
    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        try:
            import smtplib
            from email.message import EmailMessage
            import email.utils

            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT

            email_domain = str(self.fromaddr[self.fromaddr.index('@') + 1 : ])
            smtp = smtplib.SMTP(self.mailhost, port, email_domain, timeout=self.timeout)

            msg = EmailMessage()
            msg['From'] = self.fromaddr
            msg['To'] = ','.join(self.toaddrs)
            msg['Subject'] = self.getSubject(record)
            msg['Date'] = email.utils.localtime()
            msg.set_content(self.format(record))
            if self.username:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(self.username, self.password)
            smtp.send_message(msg)
            smtp.quit()
        except Exception:
            self.handleError(record)

