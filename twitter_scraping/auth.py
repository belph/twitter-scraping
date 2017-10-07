import getpass
import json
import logging
import os
import tweepy

from six.moves import input

_LOG = logging.getLogger('auth')
_LOG.setLevel(logging.INFO)
fmt = logging.Formatter("[%(levelname)s] %(name)s (%(asctime)s) - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
_LOG.addHandler(ch)
_CONFIG_FILE = os.path.expanduser("~/.tweepy/config")
_GMAIL_CONFIG = os.path.expanduser('~/.gmail-credentials')

_USE_DEFAULT = object()

__AUTH = None
__GMAIL = None

def _prompt_yes_no(msg, default=None):
    if default is None:
        default_msg = ""
    elif default:
        default_msg = " [Y]"
    else:
        default_msg = " [N]"
    prompt = "{}{}: ".format(msg, default_msg)
    err_msg = "Please input Y or N."
    while True:
        resp = input(prompt).strip().lower()
        if len(resp) > 0:
            if resp[0] == "y":
                return True
            elif resp[0] == "n":
                return False
            else:
                print(err_msg)
        elif default is not None:
            return default
        else:
            print(err_msg)


def _prompt_nonempty(msg, default=_USE_DEFAULT):
    if default is _USE_DEFAULT:
        default_msg = ""
    else:
        default_msg = " [default: {}]".format(default)
    prompt = "{}{}: ".format(msg, default_msg)
    err_msg = "Input is required."
    while True:
        resp = input(prompt).strip()
        if len(resp) > 0:
            return resp
        elif default is not _USE_DEFAULT:
            return default
        else:
            print(err_msg)


def get_auth():
    global __AUTH
    if __AUTH is not None:
        return __AUTH
    env_vars = set(['TWITTER_CONSUMER_KEY', 'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_TOKEN_SECRET'])
    if os.path.exists(_CONFIG_FILE):
        _LOG.info("Loading authentication information from configuration: {}".format(_CONFIG_FILE))
        with open(_CONFIG_FILE) as f:
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
        cfg['key'] = _prompt_nonempty("Twitter consumer key")
        prompted_any = True
    if 'secret' not in cfg:
        cfg['secret'] = _prompt_nonempty("Twitter consumer secret")
        prompted_any = True
    if 'access_token' not in cfg:
        cfg['access_token'] = _prompt_nonempty("Twitter access token")
        prompted_any = True
    if 'access_token_secret' not in cfg:
        cfg['access_token_secret'] = _prompt_nonempty("Twitter access token secret")
        prompted_any = True
    if prompted_any and _prompt_yes_no("Save configuration to a file for future use?", default=True):
        if not os.path.exists(os.path.dirname(_CONFIG_FILE)):
            os.makedirs(os.path.dirname(_CONFIG_FILE))
        with open(_CONFIG_FILE, "w") as f:
            f.write(json.dumps(cfg))
        _LOG.info("Authentication information written to configuration: {}".format(_CONFIG_FILE))
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
        cfg['username'] = _prompt_nonempty("Gmail username")
        prompted_any = True
    if 'password' not in cfg:
        # This is of course an application-specific password
        cfg['password'] = getpass.getpass("Gmail password: ")
        prompted_any = True
    if prompted_any and _prompt_yes_no("Save credentials to a file for future use?", default=True):
        with open(_GMAIL_CONFIG, "w") as f:
            f.write(json.dumps(cfg))
        _LOG.info("Gmail information written to file: {}".format(_GMAIL_CONFIG))
    __GMAIL = cfg
    return cfg
