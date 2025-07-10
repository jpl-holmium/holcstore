import datetime as dt
import logging
import pandas as pd
from typing import Union, List, Tuple

import pytz

logger = logging.getLogger(__name__)


def check_ts_completeness(ds: pd.Series, start: dt.datetime, end: dt.datetime, tag=None, freq=None, freq_margin=None,
                          msgs=None) -> List[Tuple[Union[dt.datetime, dt.date], Union[dt.datetime, dt.date]]]:
    """
    Check if timeserie ds contains full data between start and end at freq.

    Optionnally store descriptions of holes in msgs.

    :param ds: timeserie to check
    :type ds: pd.DataFrame or pd.Series
    :param start: check start
    :type start: dt.datetime
    :param end: check end
    :type end: dt.datetime
    :param tag: tag of ds for logging purpose
    :type tag: None/str
    :param freq: freq to reindex input, default 30min
    :type freq: None/str
    :param freq_margin: freq of external data store. This is used to add margin around holes datetime ranges.
    :type freq_margin: None/str
    :param msgs: if specified, warning messages will be stored inside
    :type msgs: None/list
    :return: list of tuple [(start_hole1, end_hole1), (start_hole2, end_hole2)..]
    :rtype: list
    """
    def _notify(msg):
        fullmsg = tag + msg
        if msgs is not None:
            msgs.append(fullmsg)
        else:
            logger.warning(fullmsg)

    freq = freq if freq is not None else '30min'
    freq_margin = freq_margin if freq_margin is not None else freq
    if isinstance(freq_margin, pd.Timedelta):
        timedelta_margin = freq_margin
    else:
        timedelta_margin = pd.to_timedelta(pd_freq_fix(freq_margin))
    tag = tag + ' ' if tag is not None else ''
    if ds.empty:
        _notify(f'is empty')
        return (start, end),

    ds = ds.sum(axis=1) if isinstance(ds, pd.DataFrame) else ds
    ds_ri = ds.copy().reindex(pd.date_range(start, end, freq=freq))
    ds_nulls = ds_ri.isnull()

    nulls_seqs = []
    if ds_nulls.any():
        _notify('contains NaNs')
        seqs = find_constant_sequences(ds_nulls)
        for (_from, _to), isnull in seqs:
            if isnull:
                _notify(f'  from {_from} to {_to} ({_to-_from}) (holes widen by {timedelta_margin})')
                nulls_seqs.append((_from - timedelta_margin, _to + timedelta_margin))
    return nulls_seqs


def equalp(x, y):
    """
    Equivalent of x == y, but return True for nan == nan
    """
    return (x == y) or (pd.isna(x) and pd.isna(y))


def find_constant_sequences(ds: pd.Series):
    """
    return sequences where data is constant

    :param ds: serie to check
    :type ds: pd.Series
    :return:(([idxmin1, idxmax1], value1), ([idxmin2, idxmax2], value2), ...)
    :rtype: tuple
    """
    store = []
    any_nan = ds.isnull().any()
    for index, value in ds.items():
        if not store:
            # init
            same = False
        elif any_nan:
            # slower algo because nan == nan returns False
            same = equalp(value, store[-1][1])
        else:
            same = value == store[-1][1]

        if same:
            # edit end portion
            store[-1][0][1] = index
        else:
            # init portion
            store.append(([index, index], value))
    return tuple((tuple(x), y) for x, y in store)


def split_ts(ds: pd.Series, idx_split: dt.datetime, idx_min=None, idx_max=None):
    """
    split ts in 2 ts before and after index

    :param ds: timeserie to split
    :type ds: pd.Series
    :param idx_split: index to split timeserie
    :type idx_split: dt.datetime
    :param idx_min: minimum index (must be lower than idx_split)
    :type idx_max: dt.datetime / None
    :param idx_max: maximum index (must be greater than idx_split)
    :type idx_max: dt.datetime / None
    :return: ds_before_idx, ds_after_idx
    :rtype:
    """
    if idx_min is None:
        ds_before = ds[:idx_split]
    else:
        if idx_min >= idx_split:
            raise ValueError(f'idx_min >= idx_split : {idx_min} <= {idx_split}')
        ds_before = ds[idx_min:idx_split]

    if idx_max is None:
        ds_after = ds[idx_split:]
    else:
        if idx_max <= idx_split:
            raise ValueError(f'idx_max <= idx_split : {idx_max} <= {idx_split}')
        ds_after = ds[idx_split:idx_max]
    return ds_before, ds_after


def ts_combine_first(ds_list, default_type=None):
    """
    combine time series list : apply combine_first from first list, handle empty series

    :param ds_list: list of series to combine
    :type ds_list: [pd.Series]
    :param default_type: specify default returned empty object : Series / DataFrame
    :type default_type: callable
    :return: combined serie
    :rtype: pd.Series
    """
    ds_combined = None
    default_type = default_type if default_type is not None else pd.Series
    for ds in ds_list:
        if ds is None or ds.empty:
            continue
        if ds_combined is None:
            ds_combined = ds
        else:
            try:
                ds_combined = ds_combined.combine_first(ds)
            except Exception as ex:
                logger.error(f'ts_combine_first failed')
                logger.error(f'left serie = \n{ds_combined}')
                logger.error(f'right serie = \n{ds}')
                raise ex

    return ds_combined if ds_combined is not None else default_type(dtype=float)


def upsample_using_reference_serie(ds_resampled: pd.Series, ds_reference: pd.Series):
    """
    compute dataserie from ds_resampled data (great period) using evolutions from ds_reference (small period)

    :param ds_resampled: serie to resample, must have a greater frequency than ds_reference
    :type ds_resampled: pd.Serie
    :param ds_reference: serie with lower frequency, used to compute mapping keys
    :type ds_reference: pd.Serie
    todo : robust input checks
    todo ? exploiter l'evolution intrinseque de ds_resampled ? typiquement si un nuage passe le matin en phase de montée
    :return:
    :rtype:
    """
    add_last = True
    df_work = pd.DataFrame(index=ds_reference.index)
    df_work['feat'] = ds_resampled
    df_work['idx_time'] = pd.Series(index=ds_resampled.index, data=range(0, len(ds_resampled)))
    df_work['feat'] = df_work['feat'].ffill()
    df_work['idx_time'] = df_work['idx_time'].ffill()
    df_work['ref'] = ds_reference

    ds_out = pd.Series(index=ds_reference.index, dtype=float)
    # if debug:
    #     ds_ratio_plot = pd.Series(index=ds_reference.index, data=1.)

    df_grouped = list(df_work.groupby('idx_time'))
    if add_last:
        df_grouped_new = list()
        for ii, (idx_time, df_group) in enumerate(df_grouped):
            try:
                df_next = df_grouped[ii+1][1]
            except IndexError:
                df_next = None
            if df_next is not None:
                # df_new = df_group.append(df_next.iloc[0, :])
                added_serie = df_next.iloc[0, :]
                df_new = pd.concat([df_group, added_serie.to_frame().transpose()])
            else:
                df_new = df_group
            df_grouped_new.append((idx_time, df_new))
        df_grouped = df_grouped_new

    for ii, (idx_time, df_group) in enumerate(df_grouped):
        ref_mean = df_group['ref'].mean()
        if ref_mean != 0:
            ds_ratio = df_group['ref'] / ref_mean
            ds_out[df_group.index] = ds_ratio * df_group['feat']
            # if debug:
            #     ds_ratio_plot[df_group.index] = ds_ratio
        else:
            ds_out[df_group.index] = df_group['feat']

    # if debug:
    #     plotter = Plotter()
    #     plotter.plot_feat(df_work['feat'], label='feat', marker='x')
    #     plotter.plot_feat(df_work['ref'], label='ref', marker='x')
    #     plotter.plot_feat(ds_out, label='feat reasampled', marker='x')
    #     # plotter.plot_feat(ds_ratio_plot, label='ratios computed', marker='x', secondary_axis=True)
    #     plotter.show_plot('upsample_using_reference_serie')
    return ds_out


def pd_freq_fix(freq_str: str):
    """
    Fix the syntax of frequency returned by some pandas methods.
    For instance :
        In[]:  pd.infer_freq(ds.index)
        Out[]: 'H'
        pd.to_timedelta('H') will raise a ValueError
    :param freq_str:
    :type freq_str:
    :return: fixed frequency syntax
    :rtype: str
    """
    if not isinstance(freq_str, str):
        logger.warning(f'freq_str {freq_str} is not a string')
        return freq_str
    elif freq_str[0].isnumeric():
        return freq_str
    else:
        return '1' + freq_str


def pd_min_freq(index):
    """
    Find minimum frequency in index
    :param index:
    :return:
    """
    if len(index) < 2:
        return None

    diffs = (index[1:] - index[:-1])
    min_delta = diffs.min()
    return abs(min_delta)


def ts_normalize(ds: pd.Series):
    """ normalize series to be in [0, 1] """
    # (ds - ds.mean()) / ds.std()
    return (ds - ds.min()) / (ds.max() - ds.min())


def slice_ts(ds: Union[pd.Series, pd.DataFrame], start: dt.datetime, end: dt.datetime):
    pos = ((ds.index >= start) &
           (ds.index < end))
    return ds.loc[pos]


TZ_PARIS = 'Europe/Paris'

def _localise_date(pydate, time=dt.time(), timezone_name=TZ_PARIS):
    if timezone_name is None:
        return dt.datetime.combine(pydate, time)
    else:
        return pytz.timezone(timezone_name).localize(dt.datetime.combine(pydate, time))


def _localise_date_interval(date_start, date_end, timezone_name=TZ_PARIS):
    return (_localise_date(date_start, timezone_name=timezone_name),
            _localise_date(date_end, time=dt.time.max, timezone_name=timezone_name))


def _localise_datetime(pydatetime, timezone_name=TZ_PARIS):
    return pytz.timezone(timezone_name).localize(pydatetime)


def _localised_now(timezone_name=TZ_PARIS):
    now = _localise_datetime(dt.datetime.now(), timezone_name='UTC')
    return _tz_convert_datetime(now, timezone_name=timezone_name)


def _tz_convert_datetime(pydatetime, timezone_name=TZ_PARIS):
    return pydatetime.astimezone(pytz.timezone(timezone_name))
