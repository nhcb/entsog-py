import sys
from socket import gaierror
from time import sleep
import requests
from functools import wraps
from .exceptions import NoMatchingDataError
import pandas as pd
import logging

from .misc import year_blocks, day_blocks, month_blocks, week_blocks


def retry(func):
    """Catches connection errors, waits and retries"""

    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            except (requests.ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(
                    self.retry_delay), file=sys.stderr)
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error

    return retry_wrapper


def year_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, start, end, **kwargs):
        blocks = year_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                logging.debug(f"NoMatchingDataError: between {_start} and {_end}")
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames, sort=True)
        df = df.drop_duplicates(keep = 'first')
        return df
        
    return year_wrapper


def month_limited(func):
    """Deals with calls where you cannot query more than a month, by splitting
    the call up in blocks per month"""

    @wraps(func)
    def month_wrapper(*args, start, end, **kwargs):
        blocks = month_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                logging.debug(f"NoMatchingDataError: between {_start} and {_end}")
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames, sort=True)
        df = df.drop_duplicates(keep = 'first')
        return df

    return month_wrapper


def day_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def day_wrapper(*args, start, end, **kwargs):
        blocks = day_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                print(f"NoMatchingDataError: between {_start} and {_end}", file=sys.stderr)
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)

        df = df.drop_duplicates(keep = 'first')

        return df

    return day_wrapper


def week_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def week_wrapper(*args, start, end, **kwargs):
        blocks = week_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                print(f"NoMatchingDataError: between {_start} and {_end}", file=sys.stderr)
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)
        df = df.drop_duplicates(keep = 'first')

        return df

    return week_wrapper


def operator_limited(func):
    """Deals with calls where you cannot query more than one operator, by splitting
    the call up in blocks per operator"""

    @wraps(func)
    def operator_wrapper(*args, operator, **kwargs):
        blocks = operator
        frames = []
        for _operator in blocks:
            try:
                frame = func(*args, operator = _operator, **kwargs)
            except NoMatchingDataError:
                print(f"NoMatchingDataError: {_operator}", file=sys.stderr)
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)
        return df

    return operator_wrapper
