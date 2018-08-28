import asyncio
import logging
import time


class Ticker:
    """
        yields at equal time intervals.
        can be used to perform slow ops at exact timeouts
    """
    def __init__(self, timeout):
        self.timeout = timeout
        self.last_call = time.time() - timeout

    def __aiter__(self):
        return self

    async def __anext__(self):
        now = time.time()
        clear_time = self.last_call + self.timeout
        if clear_time > now:
            await asyncio.sleep(clear_time - now)
        self.last_call = time.time()
        return


def set_log_level(logger):
    def inner(log_level):
        log_level = getattr(logging, log_level.upper())
        logger.setLevel(log_level)
    return inner


def prepare_logger(module_name):
    # TODO store logs in files, logrotate
    logger = logging.getLogger(module_name)
    log_handler = logging.StreamHandler()
    log_handler.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    logging.getLogger('aioredis').addHandler(log_handler)
    logging.getLogger('aioredlock').addHandler(log_handler)
    return logger
