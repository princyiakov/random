import time
from datetime import datetime
from functools import wraps

def timeit(logger=print, fmt="%Y-%m-%d %H:%M:%S"):
    """
    Decorator to log start time, end time, and duration for any function.
    - logger: a callable like print or logger.info
    - fmt: datetime format for start/end display
    """
    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start_wall = datetime.now()
            t0 = time.perf_counter()
            status = "OK"
            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as e:
                status = f"ERROR: {e.__class__.__name__}"
                raise
            finally:
                t1 = time.perf_counter()
                end_wall = datetime.now()
                elapsed = t1 - t0
                logger(
                    f"[{fn.__name__}] started={start_wall.strftime(fmt)} | "
                    f"ended={end_wall.strftime(fmt)} | duration={elapsed:.3f}s | {status}"
                )
        return wrapper
    return decorate
