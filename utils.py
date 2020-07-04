import asyncio


def allow_sync(func):
    """
    Allows a function which returns a future to be run either synchronously or asynchronously
    :param func:
    :return:
    """
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        try:
            asyncio.get_running_loop()
            return future
        except RuntimeError:
            return asyncio.run(future)

    return wrapper
