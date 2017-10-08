import contextlib

import smtplib

from email.mime.text import MIMEText
from email.parser import Parser

from .auth import get_gmail_info

_DEFAULT_DEFAULT_SUBJECT = "Message from twitter-scraping"

class Emailer(object):
    def __init__(self, default_subject=None):
        self._default_subject = default_subject or _DEFAULT_DEFAULT_SUBJECT
        credentials = get_gmail_info()
        self.email = credentials['username']
        self.password = credentials['password']

    @property
    def default_subject(self):
        return self._default_subject

    @default_subject.setter
    def set_default_subject(self, subject):
        self._default_subject = subject

    @contextlib.contextmanager
    def _server_connection(self):
        server = smtplib.SMTP_SSL("smtp.gmail.com")
        try:
            server.login(self.email, self.password)
            yield server
        finally:
            server.quit()
    
    def send_message(self, message, subject=None):
        with self._server_connection() as server:
            # Allow passing of template strings as subject
            message['Subject'] = (subject or self.default_subject).format(default_subject=self.default_subject)
            message['From'] = self.email
            message['To'] = self.email
            server.sendmail(message['From'], [message['To']], message.as_string())

    def send_text(self, message, subject=None):
        self.send_message(Parser().parsestr(message), subject=subject)


class DummyEmailer(object):
    def __init__(self, default_subject=None):
        self.default_subject = default_subject or _DEFAULT_DEFAULT_SUBJECT

    def send_message(self, message, subject=None):
        pass

    def send_text(self, message, subject=None):
        pass
