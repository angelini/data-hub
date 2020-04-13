import uuid

import structlog


def simplify_arg(arg):
    if isinstance(arg, uuid.UUID):
        return str(arg)
    return arg


def simplify_args(args):
    return {
        key: simplify_arg(value)
        for key, value in args.items()
    }


def info(event, args):
    log = structlog.get_logger()
    log.info(event, **simplify_args(args))
