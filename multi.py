#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import time as _time
import multitasking as _multitasking
import pandas as _pd
import traceback

from . import Ticker, utils
from . import shared


class YFinanceReqFailException(Exception):
    def __init__(self, msg, fail_dict: dict):
        self.msg = msg 
        self.fail_dict: dict = fail_dict 


def download(tickers, start=None, end=None, actions=False, threads=True,
             group_by='column', auto_adjust=False, back_adjust=False,
             progress=True, period="max", show_errors=True, interval="1d", prepost=False,
             proxy=None, rounding=False, **kwargs):
    """Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
        start: str
            Download start date string (YYYY-MM-DD) or _datetime.
            Default is 1900-01-01
        end: str
            Download end date string (YYYY-MM-DD) or _datetime.
            Default is now
        group_by : str
            Group by 'ticker' or 'column' (default)
        prepost : bool
            Include Pre and Post market data in results?
            Default is False
        auto_adjust: bool
            Adjust all OHLC automatically? Default is False
        actions: bool
            Download dividend + stock splits data. Default is False
        threads: bool / int
            How many threads to use for mass downloading. Default is True
        proxy: str
            Optional. Proxy server URL scheme. Default is None
        rounding: bool
            Optional. Round values to 2 decimal places?
        show_errors: bool
            Optional. Doesn't print errors if True
    """

    # create ticker list
    tickers = tickers if isinstance(
        tickers, (list, set, tuple)) else tickers.replace(',', ' ').split()

    tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        shared._PROGRESS_BAR = utils.ProgressBar(len(tickers), 'completed')

    # reset shared._DFS
    shared._DFS = {}
    shared._ERRORS = {}

    # download using threads
    if threads:
        if threads is True:
            threads = min([len(tickers), _multitasking.cpu_count() * 2])
        _multitasking.set_max_threads(threads)
        for i, ticker in enumerate(tickers):
            _download_one_threaded(ticker, period=period, interval=interval,
                                   start=start, end=end, prepost=prepost,
                                   actions=actions, auto_adjust=auto_adjust,
                                   back_adjust=back_adjust,
                                   progress=(progress and i > 0), proxy=proxy,
                                   rounding=rounding)
        _multitasking.wait_for_tasks()

        # 기존 yfinance에 추가한 코드. 
        # 위 multitasking.wait_for_tasks()로 스레드 완료 대기 로직을 대신할 수 있고, 
        # 예기치 못하게 스레드에서 shared._DFS를 처리하지 못하고 종료되어버리면 무한 대기가 발생할 수 있어 제거. 
        # 기존 코드에서, base.py의 history()에서 get 요청에서 오류 발생하면 shared._DFS가 처리되지 않아 무한대기 발생했었음. 
        # while len(shared._DFS) < len(tickers):
        #     _time.sleep(0.01)

    # download synchronously
    else:
        for i, ticker in enumerate(tickers):
            data = _download_one(ticker, period=period, interval=interval,
                                 start=start, end=end, prepost=prepost,
                                 actions=actions, auto_adjust=auto_adjust,
                                 back_adjust=back_adjust, proxy=proxy,
                                 rounding=rounding)
            shared._DFS[ticker.upper()] = data
            if progress:
                shared._PROGRESS_BAR.animate()

    if progress:
        shared._PROGRESS_BAR.completed()

    if shared._ERRORS and show_errors:
        print('\n%.f Failed download%s:' % (
            len(shared._ERRORS), 's' if len(shared._ERRORS) > 1 else ''))
        # print(shared._ERRORS)
        print("\n".join(['- %s: %s' %
                         v for v in list(shared._ERRORS.items())]))
        
    # 기존 yfinance에 추가한 코드. 
    # 데이터 요청 중에 문제가 있어 shared._ERRORS가 존재하는 경우, _download_one이나 _download_one_threaded에서 
    # 어차피 전처리도 안된 불완전한 데이터 반환함. 데이터 확인해서 정상수집여부 확인하느니 그냥 에러 던지도록. 
    if shared._ERRORS: 
        fail_dict: dict = {}
        for ticker, error_msg in shared._ERRORS.items():
            fail_dict[ticker] = error_msg

        raise YFinanceReqFailException(f"yfinance download fail. ", fail_dict)  

    if len(tickers) == 1:
        return shared._DFS[tickers[0]]

    try:
        data = _pd.concat(shared._DFS.values(), axis=1,
                          keys=shared._DFS.keys())
    except Exception:
        _realign_dfs()
        data = _pd.concat(shared._DFS.values(), axis=1,
                          keys=shared._DFS.keys())

    if group_by == 'column':
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level=0, axis=1, inplace=True)

    return data


def _realign_dfs():
    idx_len = 0
    idx = None

    for df in shared._DFS.values():
        if len(df) > idx_len:
            idx_len = len(df)
            idx = df.index

    for key in shared._DFS.keys():
        try:
            shared._DFS[key] = _pd.DataFrame(
                index=idx, data=shared._DFS[key]).drop_duplicates()
        except Exception:
            shared._DFS[key] = _pd.concat([
                utils.empty_df(idx), shared._DFS[key].dropna()
            ], axis=0, sort=True)

        # remove duplicate index
        shared._DFS[key] = shared._DFS[key].loc[
            ~shared._DFS[key].index.duplicated(keep='last')]


@_multitasking.task
def _download_one_threaded(ticker, start=None, end=None,
                           auto_adjust=False, back_adjust=False,
                           actions=False, progress=True, period="max",
                           interval="1d", prepost=False, proxy=None,
                           rounding=False):
    try:
        data = _download_one(ticker, start, end, auto_adjust, back_adjust,
                            actions, period, interval, prepost, proxy, rounding)
        shared._DFS[ticker.upper()] = data
        if progress:
            shared._PROGRESS_BAR.animate()
    
    # 기존 yfinance에 추가한 코드. 
    # 스레드 실행간의 처리되지 않은 예외 캐치하여 다른 예외들과 동일하게 처리해줌. 
    except Exception as e:
        shared._DFS[ticker.upper()] = utils.empty_df() 
        shared._ERRORS[ticker.upper()] = traceback.format_exc()


def _download_one(ticker, start=None, end=None,
                  auto_adjust=False, back_adjust=False,
                  actions=False, period="max", interval="1d",
                  prepost=False, proxy=None, rounding=False):

    return Ticker(ticker).history(period=period, interval=interval,
                                  start=start, end=end, prepost=prepost,
                                  actions=actions, auto_adjust=auto_adjust,
                                  back_adjust=back_adjust, proxy=proxy,
                                  rounding=rounding, many=True)
