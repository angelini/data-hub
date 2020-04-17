import datetime as dt
import enum
import logging
import pathlib
import sys
import uuid

import structlog


def simplify_arg(arg):
    if isinstance(arg, (uuid.UUID, pathlib.Path)):
        return str(arg)
    if isinstance(arg, enum.Enum):
        return arg.value
    if isinstance(arg, dt.datetime):
        return arg.strftime('%Y-%m-%dT%H:%M:%S')
    return arg


def simplify_args(args):
    return {
        key: simplify_arg(value)
        for key, value in args.items()
    }


def configure():
    logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt='%Y-%m-%d %H:%M:%S'),
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=structlog.threadlocal.wrap_dict(dict),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def init(**kwargs):
    log = structlog.get_logger()
    log.new(**simplify_args(kwargs))


def info(event, **kwargs):
    log = structlog.get_logger()
    log.info(event, **simplify_args(kwargs))


def warn(event, **kwargs):
    log = structlog.get_logger()
    log.warn(event, **simplify_args(kwargs))
