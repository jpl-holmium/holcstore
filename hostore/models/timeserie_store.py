import io
import logging
from typing import List, Dict, Union

import pandas as pd
from django.db import models
from packaging.version import Version
from pyarrow import BufferReader

from hostore.utils.timeseries import ts_combine_first

logger = logging.getLogger(__name__)
# below this version of pandas we must remove datetime index from the serie to be feathered
MIN_PANDAS_VERSION_FEATHER_SAVE_DATETIME_INDEX = '2.2.0'

class TimeseriesStore(models.Model):
    data = models.BinaryField(blank=True, null=True)

    class Meta:
        abstract = True
        # must be customized
        constraints = [models.UniqueConstraint(fields=['data'], name='%(app_label)s_%(class)s_data_orig_ctn'), ]
        indexes = [models.Index(fields=['data']), ]  # must be customized

    @property
    def decoded_ts_data(self) -> dict:
        """
        Returns a dict of all entries and timeserie ('data' key)
        """
        entry_dict = vars(self)

        # decode
        reader = BufferReader(entry_dict['data'])
        df = pd.read_feather(reader)
        # fix feather index
        if 'index' in df.columns:
            df.set_index('index', inplace=True)
        entry_dict['data'] = df.iloc[:, 0]
        return entry_dict

    @staticmethod
    def encode_serie(ds):
        """
        Encode and compress serie
        """
        # pandas to feather
        df = ds.to_frame(name='name')

        # fix feather index
        if Version(pd.__version__) < Version(MIN_PANDAS_VERSION_FEATHER_SAVE_DATETIME_INDEX):
            # remove index from serie
            df.reset_index(inplace=True, names=['index'])

        buf = io.BytesIO()
        df.to_feather(buf, compression='lz4')
        data = buf.getvalue()
        return data

    @classmethod
    def get_ts(cls, ts_attributes: dict, flat=False, _qs=None) -> Union[pd.Series, List[Dict]]:
        """
        Get the timeseries matching ts_attributes

        Args:
            ts_attributes: dict : specify attributes to get
            flat: bool : whether the query must return only one results (and return only data serie). when using flat
            option, there must be one and only one matching result
            _qs : private argument to pass ts_attributes queryset
        Returns:
            List[Dict]
        """
        full_key = repr(ts_attributes)
        logger.debug(f'GET key {full_key} in cache')

        if _qs is not None:
            qs = _qs
        else:
            qs = cls.objects.filter(**ts_attributes)

        entries = []
        for entry in qs:
            entries.append(entry.decoded_ts_data)

        logger.debug(f'GET key {full_key} in cache DONE')
        if flat:
            if len(entries) == 1:
                return entries[0]['data']
            elif len(entries) == 0:
                raise ValueError(f'No serie found for key {full_key}')
            else:
                raise ValueError(f'Multiple series found for key {full_key}')
        else:
            return entries

    @classmethod
    def set_ts(cls, ts_attributes: dict, ds_ts: pd.Series, update=False, replace=False) -> None:
        """
        Set one timeserie with ts_attributes keys

        Args:
            ts_attributes: dict : specify attributes to set
            ds_ts: pd.Series
            update: bool : if True, allow to update an existing ts (combine first existing and new one)
            replace: bool : if True, allow to replace an existing ts
        Returns:
            None
        """
        if update and replace:
            raise ValueError('update and replace are mutually exclusive')

        if ts_attributes is None:
            raise ValueError(f'ts_attributes is None')

        full_key = repr(ts_attributes)
        logger.debug(f'SET key {full_key} in cache')

        if not isinstance(ds_ts, pd.Series):
            raise ValueError(f'Cannot cache value of type {type(ds_ts)}, only pd.Series are accepted')

        if ds_ts.isnull().all():
            logger.warning(f'CACHE : Key {full_key} is ignored because data is null')
            return

        qs = cls.objects.filter(**ts_attributes)
        if qs.exists():
            if update:
                ds_ts_existing = cls.get_ts(ts_attributes, flat=True, _qs=qs)
                ds_ts_saved = ts_combine_first([ds_ts, ds_ts_existing])
            elif replace:
                cls.clear(ts_attributes)
                ds_ts_saved = ds_ts
            else:
                raise ValueError(f'Trying save over existing ts without update or replace option: {ts_attributes}')
        else:
            ds_ts_saved = ds_ts

        cls.objects.update_or_create(defaults=dict(data=cls.encode_serie(ds_ts_saved)), **ts_attributes)
        logger.debug(f'SET key {full_key} in cache DONE')

    @classmethod
    def clear(cls, custom_filters) -> None:
        """
        Method to clear multiple prm from cache
        Args:
            custom_filters: dict
        Returns:
            None
        """
        qs = cls.objects.filter(**custom_filters)
        qs.delete()
