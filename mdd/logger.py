# import
import inspect
import logging
import logging.config
import logging.handlers
import yaml
import importlib
from pathlib import Path

from mdd.environment import Environment
from mdd.helper.function import FunctionUtils as utils


class Logger:
    # intialize a logger
    @staticmethod
    def init(log_folder, log_file_name, log_timestamp, debug=False):
        function_name = inspect.currentframe().f_code.co_name
        if debug:
            print(f"function begin: {function_name}")
            print(f"log_folder: {log_folder}")
            print(f"log_file_name: {log_file_name}")
            print(f"log_timestamp: {log_timestamp}")

        logging.getLogger("py4j").setLevel(logging.ERROR)

        # get the yaml config file
        config_file = f"{Environment.root_path_metadata}/{Environment.log_metadata}"
        if debug:
            print(f"config_file: {config_file}")

        config_path = Path(config_file)
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        if debug:
            print(f"config: {config}")

        # replace the log file path with the log folder and log file name
        # filename: <log_root_path>/<log_folder>/<log_year>/<log_month>/<log_day>/<log_folder>_<log_timestamp>/<file_name>.csv
        log_root_path = Environment.root_path_log
        log_timestamp_str = utils.timestamp_to_string(log_timestamp)
        log_year = log_timestamp_str[:4]
        log_month = log_timestamp_str[5:7]
        log_day = log_timestamp_str[8:10]
        log_file_path = (
            config["handlers"]["file"]["filename"]
            .replace("<log_root_path>", log_root_path)
            .replace("<log_folder>", log_folder)
            .replace("<log_year>", log_year)
            .replace("<log_month>", log_month)
            .replace("<log_day>", log_day)
            .replace("<log_timestamp>", log_timestamp_str)
            .replace("<file_name>", log_file_name)
        )
        if debug:
            print(f"log_file_path: {log_file_path}")

        config["handlers"]["file"]["filename"] = log_file_path

        # create the log folder if it does not exist
        Path(log_file_path).parent.mkdir(parents=True, exist_ok=True)

        # initialize the logger
        logging.config.dictConfig(config)

        if debug:
            print(f"function end: {function_name}")

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

            # the email domain is mandatory for SMTP.office365.com
            email_domain = str(self.fromaddr[self.fromaddr.index("@") + 1 :])
            smtp = smtplib.SMTP(self.mailhost, port, email_domain, timeout=self.timeout)

            msg = EmailMessage()
            msg["From"] = self.fromaddr
            msg["To"] = ",".join(self.toaddrs)
            msg["Subject"] = self.getSubject(record)
            msg["Date"] = email.utils.localtime()
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