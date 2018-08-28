import asyncio
import logging

import aioredis
import aioredlock

from redis_etude import settings


class RedisMixin:
    """
        aioredis is used as a redis client. It could improve app performance significantly,
        especially in case of slow message / error handling operations
        (which are not slow now, but might get slow later as app is (hypothetically) developed)

        aioredlock lib is used for redis multi-master support
        (which is not implemented, but is easy to implement now).
        It requires a separate connection to redis which is a trait of this library, I guess.

    """
    def __init__(self, _settings, loop, logger=None):
        self.settings: settings.Settings = _settings

        self._connection: aioredis.Redis = None
        self._lock_manager: aioredlock.Aioredlock = None
        self._lock: aioredlock.Lock = None
        self._loop = loop

        self.logger = logger or logging.getLogger()

    async def _async_del(self):
        if self._connection and not self._connection.closed:
            self._connection.close()
            await self._connection.wait_closed()
        if self._lock_manager:
            if self._lock and self._lock.valid:
                await self._lock_manager.unlock(self._lock)
            await self._lock_manager.destroy()

    async def _become_a_gen(self):
        lock_manager = await self._get_lock_manager()
        try:
            self.logger.info('Trying to acquire generator lock...')
            self._lock = await lock_manager.lock(self.settings.generator_lock)
            self.logger.info('Generator lock acquired.')
            return True
        except aioredlock.LockError:
            self.logger.info('Failed to acquire generator lock.')
            return False

    async def _extend_gen_lock(self):
        try:
            if not self._lock or not self._lock.valid:
                if not await self._become_a_gen():
                    return False
            lock_manager = await self._get_lock_manager()
            self.logger.info('Trying to extend generator lock...')
            await lock_manager.extend(self._lock)
            self.logger.info('Lock extended.')
            return True
        except aioredlock.LockError:
            self.logger.info('Failed to extend the lock.')
            return False

    async def _rpush(self, collection, item):
        return await self._conn_execute('rpush', collection, item)

    async def _lpop(self, collection):
        return await self._conn_execute('lpop', collection)

    async def _blpop(self, collection, timeout):
        return await self._conn_execute('blpop', collection, timeout=timeout)

    async def _conn_execute(self, method, *args, **kwargs):
        while True:
            conn = await self._get_connection()
            assert conn
            try:
                return await (getattr(conn, method))(*args, **kwargs)
            except aioredis.errors.RedisError as exc:
                self.logger.warning('Redis connection method "%s" failed.', method)
                self.logger.exception(exc)
                try:
                    self._connection.close()
                except Exception as exc:
                    self.logger.debug(exc)
                self._connection = None
                await asyncio.sleep(self.settings.connection_timeout)

    async def _get_connection(self) -> aioredis.Redis:
        if self._connection:
            return self._connection
        while True:
            try:
                self.logger.info('Connecting to %s', self.settings.redis_address)
                self._connection = await aioredis.create_redis(
                    self.settings.redis_address, loop=self._loop,
                    timeout=self.settings.connection_timeout
                )
                self.logger.info('Connected.')
                return self._connection
            except Exception as exc:
                self.logger.warning('Connection failed.')
                self.logger.exception(exc)
                await asyncio.sleep(self.settings.reconnect_timeout)

    async def _get_lock_manager(self) -> aioredlock.Aioredlock:
        if self._lock_manager:
            return self._lock_manager
        while True:
            try:
                self.logger.info('Creating lock-handling connection.')
                self._lock_manager = aioredlock.Aioredlock(self.settings.redis_instances,
                                                           lock_timeout=self.settings.generator_timeout)
                self.logger.info('Lock-handling client connected.')
                return self._lock_manager
            except Exception as exc:
                self.logger.warning('Lock-handling connection failed.')
                self.logger.exception(exc)
                await asyncio.sleep(self.settings.reconnect_timeout)
