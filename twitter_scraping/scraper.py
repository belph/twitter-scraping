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

_LOG = logging.getLogger('scraper')
fmt = logging.Formatter("[%(levelname)s] %(name)s (%(asctime)s) - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
_LOG.addHandler(ch)
ch = logging.FileHandler("scraper.log", mode="w")
ch.setFormatter(fmt)
_LOG.addHandler(ch)
_LOG.setLevel(logging.INFO)

class _ElapsedTime(object):
    def __init__(self, total_seconds):
        self._total_seconds = total_seconds

    @property
    def total_seconds(self):
        return self._total_seconds

    @property
    def seconds(self):
        return int(self._total_seconds % 60)

    @property
    def minutes(self):
        return int((self._total_seconds / 60) % 60)

    @property
    def hours(self):
        return int((self._total_seconds / 3600) % 24)

    @property
    def days(self):
        return int(self._total_seconds / 86400)

    def format(self, fmt):
        return fmt.format(
            days=self.days,
            hours=self.hours,
            minutes=self.minutes,
            seconds=self.seconds,
            total_seconds=self.total_seconds,
            total_seconds_int=int(self.total_seconds))
    
    def __str__(self):
        return self.format("{days}d{hours}h{minutes}m{seconds}s [{total_seconds_int}s]")

class ScraperStreamListener(tweepy.StreamListener):
    def __init__(self, output_file, emailer=None, notify_count=None, notify_frequency=None, *args, **kwargs):
        super(ScraperStreamListener, self).__init__(*args, **kwargs)
        self._emailer = emailer
        self._output = open(output_file, "w", encoding="utf-8")
        self._cache = LRUCache(maxsize=1000)
        self._num_written = 0
        self._log_frequency = 60 * 15 # Write log message every 15min
        self._notify_frequency = notify_frequency
        self._notify_count = notify_count
        self._start = time.time()
        self._last_notification = time.time()
        self._last_notification_count = 0
        self._last_log_notification = time.time()
        self._rate_limit_errors = 0
        self._other_errors = 0
        self._milestone_size = 1000000
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
        return _ElapsedTime(time.time() - self._start)

    @property
    def next_milestone(self):
        return (int(self._num_written / self._milestone_size) + 1) * self._milestone_size
    
    def notify(self, send_email=False):
        self._last_log_notification = time.time()
        elapsed = self.elapsed
        next_milestone = self.next_milestone
        remaining_to_milestone = next_milestone - self._num_written
        rate = self._num_written / float(elapsed.total_seconds)
        eta = _ElapsedTime(remaining_to_milestone / rate)
        message = "{:,} tweets have been collected so far (time elapsed: {}). The collection rate is an average of {} tweets/min. ETA to {:,} tweets: {}".format(self._num_written, elapsed, rate * 60, next_milestone, eta)
        _LOG.info(message)
        if send_email:
            self._last_notification = time.time()
            self._last_notifiaction_count = self._num_written
            self._emailer.send_text(message=message)

    def notify_if_needed(self):
        send_email = False
        if self._notify_count is not None \
           and self._num_written % self._notify_count == 0 \
           and self._num_written != self._last_notification_count:
            send_email = True
        elif self._notify_frequency is not None and time.time() - self._last_notification >= self._notify_frequency:
            send_email = True

        if send_email or time.time() - self._last_log_notification > self._log_frequency:
            self.notify(send_email=send_email)

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
        self._emailer = None

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
        with ScraperStreamListener(emailer=self._emailer,
                                   output_file=self._output_file,
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

    def emailer(self, emailer):
        self._emailer = emailer
        return self


