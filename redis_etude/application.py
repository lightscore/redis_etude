#!/usr/bin/env python

import argparse
import asyncio
import logging
import random
import string
import sys
import time

from redis_etude import mixin
from redis_etude import settings
from redis_etude import util


logger = util.prepare_logger(__name__)

if sys.version_info < (3, 5):
    logger.critical('This program uses features from python3.5+ (tested on python3.7). '
                    'Please update your Python interpreter.')
    exit(1)


class App(mixin.RedisMixin):
    """
        Generator / receiver app with sync interface and async implementation.

        Some methods (gen_msg, handle_err) are async even though they don't need to be.
        That's because I suspect they might be slow in a real-world app.
    """
    def __init__(self, _settings, loop=None, _logger=None):
        self.mark = ''.join(random.choices(string.hexdigits, k=8))
        self.logger = _logger or logging.getLogger(__name__ + self.mark)
        self.logger.info('Application %s is created.', self.mark)

        self._start_time = None
        self._stop = False

        self.sent_messages = []
        self.sent_errors = []
        self.received_messages = []
        self.received_errors = []

        loop = loop or asyncio.get_event_loop()
        super().__init__(_settings, loop, self.logger)

    def __del__(self):
        self._loop.run_until_complete(self._async_del())

    def run(self):
        """ start the app in event loop"""
        self._loop.run_until_complete(self.async_run())

    def stop(self):
        logger.info('Stopping the app...')
        self._stop = True

    async def async_run(self):
        self._start_time = time.time()
        while self._keep_going():
            if await self._become_a_gen():
                await self.async_run_gen()
            else:
                await self.async_run_receiver()
        logger.info('Application stopped.')

    async def async_run_gen(self):
        """ run until fail to extend the lock """
        logger.info('Running a generator...')
        async for _ in util.Ticker(self.settings.generator_timeout):
            if not self._keep_going():
                return
            if await self._extend_gen_lock():
                msg = await self._gen_msg()
                await self._post_msg(msg)
            else:
                logger.info('Failed to extend gen lock. Stopping the generator.')
                return

    async def async_run_receiver(self):
        """
            run until there are messages to handle

            wait for new message for the timeout from settings.
            it may be a good idea to count receivers via redis and adjust the timeout
        """
        logger.info('Running a receiver...')
        while self._keep_going():
            msg = await self._get_msg()
            if msg:
                await self._handle_msg(msg)
            else:
                logger.info('No messages received. Halting the receiver.')
                return

    def get_errors(self):
        self._loop.run_until_complete(self.async_get_errors())

    async def async_get_errors(self):
        err_count = 0
        err = await self._get_err()
        while err:
            asyncio.ensure_future(self._handle_err(err), loop=self._loop)
            err_count += 1
            err = await self._get_err()
        logger.info('%i error(s) retrieved', err_count)

    def _keep_going(self):
        """ check if app shall continue with execution or stop"""
        if self._stop:
            return False
        if not self.settings.lifetime:
            return True
        return self._start_time + self.settings.lifetime > time.time()

    async def _get_msg(self):
        """ retrieve a message (or None) from redis messages list """
        result = await self._blpop(self.settings.message_collection, timeout=self.settings.get_timeout)
        if result:
            _, msg = result
            return msg
        return result

    async def _post_msg(self, msg):
        """ post a message to redis message list """
        logger.info('Posting message "%s"...', msg)
        result = await self._rpush(self.settings.message_collection, msg)
        if self.settings.save_history:
            self.sent_messages.append(msg)
        logger.info('Message "%s" posted. %i message(s) are waiting in db.', msg, result)

    async def _get_err(self):
        """ retrieve an error from redis error list """
        return await self._lpop(self.settings.error_collection)

    async def _handle_msg(self, msg, err_rate=.05):
        """ handle a retrieved message """
        logger.info('Message received: %s', msg)
        if self.settings.save_history:
            self.received_messages.append(msg)
        if random.random() < err_rate:
            asyncio.ensure_future(self._handle_msg_err(msg), loop=self._loop)

    async def _handle_msg_err(self, msg):
        """ handle a retrieved message which is found to contain an error """
        logger.warning('Error in received message: "%s"', msg)
        await self._rpush(self.settings.error_collection, msg)
        if self.settings.save_history:
            self.sent_errors.append(msg)
        logger.info('Erroneous message "%s" stored in redis', msg)

    async def _handle_err(self, err):
        """ handle a retrieved error """
        logger.info('Handling error: "%s"', err)
        if self.settings.save_history:
            self.received_errors.append(err)

    async def _gen_msg(self):
        """ generate a new message to post """
        return ''.join(random.choices(string.hexdigits, k=32))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser("Generator / receiver redis-based app")
    arg_parser.add_argument('--getErrors', dest='get_errors', action='store_true')
    arg_parser.add_argument('--logLevel', type=util.set_log_level(logger))
    arg_parser.add_argument('--redisHost', dest='redis_host', default=settings.Settings.redis_host)
    arg_parser.add_argument('--redisPort', dest='redis_port', type=int, default=settings.Settings.redis_port)
    arg_parser.add_argument('--debug', action='store_true')
    args = arg_parser.parse_args()

    random.seed()

    _settings = settings.Settings(redis_host=args.redis_host, redis_port=args.redis_port)
    app = App(_settings, _logger=logger)
    if args.get_errors:
        app.get_errors()
    else:
        app.run()
