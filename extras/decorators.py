from functools import wraps
from time import sleep, time
from typing import Any, Callable


def timer_with_sleep(
    sleep_in_seconds: int,
) -> Any:  # here we are defining the decorator parameters
    def decorator(
        f: Callable[[Any], Any],
    ):  # here we are defining the function that will be decorated
        @wraps(f)
        def wrapper(
            *args, **kwargs
        ):  # here we are defining the wrapper function that will be called when the decorated function is called
            start = time()
            sleep(sleep_in_seconds)
            result = f(*args, **kwargs)
            print(
                f"Function {f.__name__} took {time() - start} seconds, including {sleep_in_seconds} sleep time"
            )

            return result

        return wrapper

    return decorator


@timer_with_sleep(sleep_in_seconds=1)
def heavy_computation(n: int) -> int:
    """
    This function simulates a heavy computation by sleeping for n seconds.
    Parameters
    ----------
    n : int
        Number of seconds to sleep

    Returns
    -------
    int
        The input number n
    """
    sleep(n)
    return n


if __name__ == "__main__":
    x = heavy_computation(1)
    print(f"Docstring of heavy_computation: {heavy_computation.__doc__}")
    print(x)
    # Output:
    # Function heavy_computation took 2.000276565551758 seconds, including 1 sleep time
    # Docstring of heavy_computation:
    #     This function simulates a heavy computation by sleeping for n seconds.
    #     Parameters
    #     ----------
    #     n : int
    #         Number of seconds to sleep
    #
    #     Returns
    #     -------
    #     int
    #         The input number n

    # 1
