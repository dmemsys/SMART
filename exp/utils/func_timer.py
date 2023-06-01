import time

from utils.color_printer import print_OK


def print_func_time(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        func(*args, **kwargs)
        execute_time = time.perf_counter() - t
        print_OK(f'Execution Time: {execute_time:.2f} s')
        return execute_time
    return fun
