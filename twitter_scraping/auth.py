import boto3
import getpass
import json
import os
import tweepy

from six.moves import input
from .log import get_logger
from .utils import prompt_yes_no, prompt_nonempty

_LOG = get_logger('auth')

_TWITTER_CONFIG = os.path.expanduser("~/.twitter-scraping/twitter-credentials")
_GMAIL_CONFIG = os.path.expanduser('~/.twitter-scraping/gmail-credentials')


__AUTH = None
__GMAIL = None


def get_auth():
    global __AUTH
    if __AUTH is not None:
        return __AUTH
    env_vars = set(['TWITTER_CONSUMER_KEY', 'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_TOKEN_SECRET'])
    if os.path.exists(_TWITTER_CONFIG):
        _LOG.info("Loading authentication information from configuration: {}".format(_TWITTER_CONFIG))
        with open(_TWITTER_CONFIG) as f:
            cfg = json.loads(f.read())
    elif all(k in os.environ for k in env_vars):
        _LOG.info("Loading authentication information from the environment.")
        cfg = {}
        cfg['key'] = os.environ['TWITTER_CONSUMER_KEY']
        cfg['secret'] = os.environ['TWITTER_CONSUMER_SECRET']
        cfg['access_token'] = os.environ['TWITTER_ACCESS_TOKEN']
        cfg['access_token_secret'] = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    else:
        in_environment = env_vars.intersection(set(list(os.environ)))
        if len(in_environment) > 0:
            not_in_environment = env_vars.difference(in_environment)
            is_are = "is" if len(not_in_environment) == 1 else "are"
            _LOG.warn("Warning: Variables {} in environment, but {} {} missing".format(", ".join(in_environment), ", ".join(not_in_environment), is_are))
        _LOG.info("Authentication information not found. Prompting.")
        cfg = {}
    prompted_any = False
    if 'key' not in cfg:
        cfg['key'] = prompt_nonempty("Twitter consumer key")
        prompted_any = True
    if 'secret' not in cfg:
        cfg['secret'] = prompt_nonempty("Twitter consumer secret")
        prompted_any = True
    if 'access_token' not in cfg:
        cfg['access_token'] = prompt_nonempty("Twitter access token")
        prompted_any = True
    if 'access_token_secret' not in cfg:
        cfg['access_token_secret'] = prompt_nonempty("Twitter access token secret")
        prompted_any = True
    if prompted_any and prompt_yes_no("Save configuration to a file for future use?", default=True):
        if not os.path.exists(os.path.dirname(_TWITTER_CONFIG)):
            os.makedirs(os.path.dirname(_TWITTER_CONFIG))
        file_already_existed = os.path.exists(_TWITTER_CONFIG)
        with open(_TWITTER_CONFIG, "w") as f:
            f.write(json.dumps(cfg))
        if not file_already_existed:
            os.chmod(_TWITTER_CONFIG, 0o600)
        _LOG.info("Authentication information written to configuration: {}".format(_TWITTER_CONFIG))
    auth = tweepy.OAuthHandler(cfg['key'], cfg['secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    __AUTH = auth
    return auth

def get_gmail_info():
    global __GMAIL
    if __GMAIL is not None:
        return __GMAIL
    if os.path.exists(_GMAIL_CONFIG):
        _LOG.info("Loading Gmail information from file: {}".format(_GMAIL_CONFIG))
        with open(_GMAIL_CONFIG) as f:
            cfg = json.loads(f.read())
    else:
        cfg = {}
    prompted_any = False
    if 'username' not in cfg:
        cfg['username'] = prompt_nonempty("Gmail username")
        prompted_any = True
    if 'password' not in cfg:
        # This is of course an application-specific password
        cfg['password'] = getpass.getpass("Gmail password: ")
        prompted_any = True
    if prompted_any and prompt_yes_no("Save credentials to a file for future use?", default=True):
        if not os.path.exists(os.path.dirname(_GMAIL_CONFIG)):
            os.makedirs(os.path.dirname(_GMAIL_CONFIG))
        file_already_existed = os.path.exists(_GMAIL_CONFIG)
        with open(_GMAIL_CONFIG, "w") as f:
            f.write(json.dumps(cfg))
        if not file_already_existed:
            os.chmod(_GMAIL_CONFIG, 0o600)
        _LOG.info("Gmail information written to file: {}".format(_GMAIL_CONFIG))
    __GMAIL = cfg
    return cfg

def check_boto_credentials():
    if boto3.Session().get_credentials() is None:
        raise RuntimeError("Failed to find AWS credentials. Please set up boto3 credentials before using: https://boto3.readthedocs.io/en/latest/guide/configuration.html")
