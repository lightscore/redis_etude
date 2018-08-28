# Redis Distributed Producer / Consumer App

Etude app that connects to redis and either becomes a producer and posts messages into queue, 
or, failing to acquire generator lock, becomes a consumer and handles messages from queue. 

Python 3.5+ is required (tested with Python 3.7).

App uses `aioredis` and `aioredlock` async clients.
App has sync and async interfaces.

## Setup
To simply install and run:
```
pip install -r requirements.txt
./application.py --redisHost=HOST --redisPort=PORT [--getErrors] [--logLevel=LVL]
```
To setup as a python package:
```
python setup.py install
```

## Tests

Tests are semi-integration and are based on `redislite` embedded redis.

Apps run in the same loop in tests for now. Maybe it would make sense to run them in separate 
threads or processes.

Run tests:
```
pip install -r requirements.txt
pytest
```
