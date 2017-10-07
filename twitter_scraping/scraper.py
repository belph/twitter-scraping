import logging
import json
import sys
import time
import tweepy

from cachetools import LRUCache
from contextlib import contextmanager
from io import open
from tweepy.models import Model

from .auth import get_auth
from . import email

_LOG = logging.getLogger('scraper')
fmt = logging.Formatter("[%(levelname)s] %(name)s (%(asctime)s) - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
_LOG.addHandler(ch)
ch = logging.FileHandler("scraper.log", mode="w")
ch.setFormatter(fmt)
_LOG.addHandler(ch)
_LOG.setLevel(logging.INFO)

class ScraperStreamListener(tweepy.StreamListener):
    def __init__(self, output_file, notify_count=None, notify_frequency=None, *args, **kwargs):
        super(ScraperStreamListener, self).__init__(*args, **kwargs)
        self._emailer = email.Emailer()
        self._output = open(output_file, "w", encoding="utf-8")
        self._cache = LRUCache(maxsize=1000)
        self._num_written = 0
        self._notify_frequency = notify_frequency
        self._notify_count = notify_count
        self._start = time.time()
        self._last_notification = time.time()
        self._last_notification_count = 0
        self._rate_limit_errors = 0
        self._other_errors = 0
        _LOG.info("Starting collection.")

    def on_error(self, status_code):
        if status_code == 420:
            _LOG.debug("Rate limited.")
            self._rate_limit_errors += 1
            if self._rate_limit_errors >= 3:
                _LOG.error("Too many rate limit errors. Closing at {}.".format(time.strftime("%Y-%m-%d %H:%M:%S")))
                self._emailer.send_text(
                    message="Disconnected scraper due to error code: {}".format(status_code),
                    subject="[ERROR] {default_subject}")
                return False
        else:
            _LOG.error("Error code received: {}".format(status_code))
            self._other_errors += 1
            if self._other_errors >= 2:
                _LOG.error("Too many other errors in a row. Closing at {}.".format(time.strftime("%Y-%m-%d %H:%M:%S")))
                self._emailer.send_text(
                    message="Disconnected scraper due to error code: {}".format(status_code),
                    subject="[ERROR] {default_subject}")
                return False

    @property
    def elapsed(self):
        return time.time() - self._start
    
    def notify(self):
        self._last_notification = time.time()
        self._last_notifiaction_count = self._num_written
        elapsed = self.elapsed
        rate = (self._num_written / float(elapsed)) * 60
        seconds = int(elapsed % 60)
        minutes = int((elapsed / 60) % 60)
        hours = int((elapsed / 3600) % 24)
        days = int(elapsed / (3600 * 24))
        message = "{} tweets have been collected so far (time elapsed: {}d{}h{}m{}s). The collection rate is an average of {} tweets/min.".format(self._num_written, days, hours, minutes, seconds, rate)
        _LOG.info("Sending notification: {}".format(message))
        self._emailer.send_text(message=message)

    def notify_if_needed(self):
        if self._notify_count is not None \
           and self._num_written % self._notify_count == 0 \
           and self._num_written != self._last_notification_count:
            self.notify()
        elif self._notify_frequency is not None and time.time() - self._last_notification >= self._notify_frequency:
            self.notify()

    def on_status(self, status):
        self._rate_limit_errors = 0
        self._other_errors = 0
        # Flatten retweets
        if hasattr(status, 'retweeted_status'):
            status = status.retweeted_status
        if status.id_str not in self._cache:
            self._cache[status.id_str] = True
            self._output.write(json.dumps(status._json))
            self._output.write("\n")
            self._num_written += 1
        self.notify_if_needed()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._output.close()

class ScraperBuilder(object):
    def __init__(self):
        self._follow = None
        self._track = None
        self._async = False
        self._locations = None
        self._stall_warnings = False
        self._languages = None
        self._encoding = 'utf8'
        self._filter_level = None
        self._output_file = None
        self._notify_count = None
        self._notify_seconds = None

    @classmethod
    def load_config(cls, config_file):
        with open(config_file, encoding="utf-8") as f:
            cfg = json.loads(f.read())
            ret = cls()
            for field in cfg:
                getattr(ret, field)(cfg[field])
            return ret

    @contextmanager
    def build(self):
        if all(x is None for x in [self._follow, self._track, self._locations]):
            print("'follow', 'track' or 'locations' is required.")
            sys.exit(1)
        if self._output_file is None:
            print("Output file is required.")
            sys.exit(1)
        with ScraperStreamListener(output_file=self._output_file,
                                   notify_frequency=self._notify_seconds,
                                   notify_count=self._notify_count) as listener:
            auth = get_auth()
            stream = tweepy.Stream(auth, listener)
            yield stream.filter(
                follow=self._follow,
                track=self._track,
                async=self._async,
                locations=self._locations,
                stall_warnings=self._stall_warnings,
                languages=self._languages,
                encoding=self._encoding,
                filter_level=self._filter_level)

    def follow(self, follow):
        self._follow = follow
        return self

    def track(self, track):
        self._track = track
        return self

    def async(self, async):
        self._async = async
        return self

    def locations(self, locations):
        self._locations = locations
        return self

    def stall_warnings(self, stall_warnings):
        self._stall_warnings = stall_warnings
        return self

    def languages(self, languages):
        self._languages = languages
        return self

    def encoding(self, encoding):
        self._encoding = encoding
        return self

    def filter_level(self, filter_level):
        self._filter_level = filter_level
        return self

    def output_file(self, output_file):
        self._output_file = output_file
        return self

    def notify_count(self, notify_count):
        assert notify_count is None or notify_count > 0, "notify_count must be greater than zero"
        self._notify_count = notify_count
        return self

    def notify_seconds(self, notify_seconds):
        assert notify_seconds is None or notify_seconds > 0, "notify_seconds must be greater than zero"
        self._notify_seconds = notify_seconds
        return self


