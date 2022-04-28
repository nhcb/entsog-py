import re
from itertools import tee
import pandas as pd
from dateutil import rrule
from unidecode import unidecode


def year_blocks(start, end):
    """
    Create pairs of start and end with max a year in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.YEARLY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def month_blocks(start, end):
    """
    Create pairs of start and end with max a month in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.MONTHLY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def week_blocks(start, end):
    """
    Create pairs of start and end with max a week in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.WEEKLY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def day_blocks(start, end):
    """
    Create pairs of start and end with max a day in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.DAILY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def pairwise(iterable):
    """
    Create pairs to iterate over
    eg. [A, B, C, D] -> ([A, B], [B, C], [C, D])

    Parameters
    ----------
    iterable : iterable

    Returns
    -------
    iterable
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def to_snake_case(string: str) -> str:
    """Converts any string to snake case

    Parameters
    ----------
    name : str

    Returns
    -------
    str
    """
    string = unidecode(string)
    string = re.sub('[^A-Za-z0-9 _]+', '_', string)
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string)
    string = re.sub('_+', '_', string.replace(' ', '_'))
    string = string.lower().replace('m_wh', 'mwh').lstrip('_').rstrip('_')
    return string
