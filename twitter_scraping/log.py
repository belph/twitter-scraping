import logging

_FMT = logging.Formatter("[%(levelname)s] %(name)s (%(asctime)s) - %(message)s")
_FILE_HANDLER = logging.FileHandler("scraper.log", "w")
_STREAM_HANDLER = logging.StreamHandler()
_NAMES = set()

for handler in [_FILE_HANDLER, _STREAM_HANDLER]:
    handler.setFormatter(_FMT)

def get_logger(name):
    log = logging.getLogger(name)
    if name not in _NAMES:
        log.addHandler(_STREAM_HANDLER)
        log.addHandler(_FILE_HANDLER)
        log.setLevel(logging.INFO)
        _NAMES.add(name)
    return log
