""" Tests use apps with limited lifetimes for the ease of setup and checking results. """

import asyncio
import contextlib
import copy
import logging

import aioredlock
import pytest
import redislite

from redis_etude import application
from redis_etude import settings
from redis_etude import util


logger = util.prepare_logger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture
def redislite_server(monkeypatch, unused_tcp_port) -> redislite.Redis:
    monkeypatch.setattr(redislite.client.RedisMixin, 'start_timeout', 2)
    redis = redislite.Redis(serverconfig={'port': unused_tcp_port})
    yield redis
    redis._cleanup()


@pytest.fixture
def redislite_address(redislite_server):
    return 'localhost', redislite_server.server_config['port']


@pytest.fixture
def acquire_gen_lock(redislite_server, redislite_address):

    @contextlib.contextmanager
    def inner():
        lock_manager = aioredlock.Aioredlock([redislite_address], lock_timeout=100)
        run_async = asyncio.get_event_loop().run_until_complete
        lock = run_async(lock_manager.lock(settings.Settings.generator_lock))
        yield
        run_async(lock_manager.unlock(lock))

    return inner


@pytest.fixture
def messages():
    return ['test_%i' % n for n in range(100)]


@pytest.fixture
def put_messages(redislite_server, messages):
    for msg in messages:
        redislite_server.rpush(settings.Settings.message_collection, msg)


@pytest.fixture
def redislite_app_factory(monkeypatch, redislite_server, redislite_address, messages):
    host, port = redislite_address

    copy_messages = copy.copy(messages)

    async def _gen_msg():
        return copy_messages.pop(0) if copy_messages else 'EXHAUSTED'

    def create_app(**kwargs):
        _settings = settings.Settings(redis_host=host, redis_port=port, save_history=True, **kwargs)
        app = application.App(_settings, _logger=logger)
        monkeypatch.setattr(app, '_gen_msg', _gen_msg)
        return app

    return create_app


def list_starts_with(xs, ys):
    for x, y in zip(xs, ys):
        if x != y:
            return False
    return True


def list_starts_with_set(xs, ss):
    return set(xs[:len(ss)]) == ss


def bytes_list(msgs):
    result = []
    for msg in msgs:
        result.append(bytes(msg, encoding='utf8'))
    return result


def run_simultaneously(*apps):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*[app.async_run() for app in apps]))


def test_single_generator(redislite_server, redislite_app_factory, messages):
    app = redislite_app_factory(lifetime=5)

    app.run()

    original_messages = bytes_list(messages)
    redis_messages = redislite_server.lrange(app.settings.message_collection, 0, -1)
    assert list_starts_with(original_messages, redis_messages)


def test_single_receiver(redislite_server, redislite_app_factory,
                         acquire_gen_lock, messages, put_messages):
    app = redislite_app_factory(lifetime=5)

    with acquire_gen_lock():
        app.run()

    assert app.received_messages == bytes_list(messages)


def test_two_receivers(redislite_server, redislite_app_factory,
                       acquire_gen_lock, messages, put_messages):
    app_1 = redislite_app_factory(lifetime=5)
    app_2 = redislite_app_factory(lifetime=5)

    with acquire_gen_lock():
        run_simultaneously(app_1, app_2)

    assert set(app_1.received_messages) & set(app_2.received_messages) == set()
    assert set(app_1.received_messages) | set(app_2.received_messages) == set(bytes_list(messages))


def test_gen_and_two_receivers(redislite_app_factory, messages):
    apps = [
        redislite_app_factory(lifetime=5),
        redislite_app_factory(lifetime=5),
        redislite_app_factory(lifetime=5),
    ]

    run_simultaneously(*apps)

    recv_app_1, recv_app_2 = [app for app in apps if not app.sent_messages]

    assert set(recv_app_1.received_messages) & set(recv_app_2.received_messages) == set()
    assert list_starts_with_set(bytes_list(messages),
                                set(recv_app_1.received_messages) | set(recv_app_2.received_messages))


def test_three_gens(redislite_server, redislite_app_factory, messages, acquire_gen_lock):
    """
        three apps take turns as a gen, others (still alive) acting as receivers
    """
    apps = [
        redislite_app_factory(lifetime=10),
        redislite_app_factory(lifetime=20),
        redislite_app_factory(lifetime=30),
    ]

    run_simultaneously(*apps)

    def sum_collections(apps, collection_name):
        result = []
        for app in apps:
            result.extend(getattr(app, collection_name))
        return result

    sent_messages = sum_collections(apps, 'sent_messages')
    received_messages = sum_collections(apps, 'received_messages')
    messages_in_redis = redislite_server.lrange(settings.Settings.message_collection, 0, -1)
    assert list_starts_with(messages, sent_messages)
    assert set(bytes_list(sent_messages)) == set(received_messages) | set(messages_in_redis)

    # first generator app
    apps_that_didnt_receive = [app for app in apps if not app.received_messages]
    assert len(apps_that_didnt_receive) == 1
    app_1 = apps_that_didnt_receive[0]

    with acquire_gen_lock():
        app_1.run()
    assert app_1.received_messages == messages_in_redis

    sent_errors = sum_collections(apps, 'sent_errors')
    app_1.get_errors()
    assert set(app_1.received_errors) == set(sent_errors)
