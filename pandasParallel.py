#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    @Time      : 2022/3/18 16:25
    @Author    : fab
    @File      : pandasParallel.py
    @Email     : xxxx@etransfar.com
"""
import swifter
# import pandas as pd
import modin.pandas as pd
import numpy as np
from pandarallel import pandarallel
import time
import warnings
warnings.filterwarnings("ignore")
# pandarallel.initialize(nb_workers=4)

swifter.register_modin()


def func(x):
    return x**3


if __name__ == '__main__':

    df = pd.DataFrame(np.random.rand(50000, 10))
    # times = []
    # for _ in range(3):
    #     start_time = time.time()
    #     df.parallel_apply(func, axis=1)
    #     times.append(time.time() - start_time)
    # print(np.mean(times), np.std(times))   # 34.92225742340088 1.5928292864816183  100w
    times = []
    for _ in range(3):
        start_time = time.time()
        out = df.swifter.apply(func, axis=1)
        times.append(time.time() - start_time)
        print(df.head(2))
        print(out.head(2))
    print(np.mean(times), np.std(times))   # 4.583480437596639 0.04223777051551772  100w


# if __name__ == "__main__":
#     df = pd.DataFrame(np.random.rand(100000, 10))
#     # 就是这么easy的调用方式
#     times = []
#     for _ in range(3):
#         start_time = time.time()
#         df.parallel_apply(func, axis=1)
#         times.append(time.time() - start_time)
#     print(np.mean(times), np.std(times))
#
#     times = []
#     for _ in range(3):
#         start_time = time.time()
#         df.apply(func, axis=1)
#         times.append(time.time() - start_time)
#     print(np.mean(times), np.std(times))
