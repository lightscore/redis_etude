
class BaseSettings:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Settings(BaseSettings):
    generator_timeout = .5  # 500 ms

    # connection_timeout is used for multiple purposes both within and outside of aioredis client
    connection_timeout = 1
    # maybe exponential retry timeout and/or some retry limits could be useful
    reconnect_timeout = 1

    # get_timeout should be adjusted to (expected number of receivers * generator_timeout) for better performance.
    # or, better yet, it can be synced with the real current number of receivers via redis
    get_timeout = 5

    redis_host = 'localhost'
    redis_port = 6379

    message_collection = 'msg'
    error_collection = 'err'
    generator_lock = 'gen'

    # for testing. 0 is infinite
    lifetime = 0

    # for testing
    save_history = False

    @property
    def redis_address(self):
        return 'redis://{}:{}'.format(self.redis_host, self.redis_port)

    @property
    def redis_instances(self):
        return [(self.redis_host, self.redis_port)]